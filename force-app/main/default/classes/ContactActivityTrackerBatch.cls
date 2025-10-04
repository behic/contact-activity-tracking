/**
 * Created by behicacar on 4.10.2025.
 */

public without sharing class ContactActivityTrackerBatch implements Database.Batchable<SObject>, Database.Stateful, Schedulable {
    private final List<String> failedContactIds = new List<String>();

    public static String scheduleDaily(String jobName) {
        jobName = String.isBlank(jobName) ? 'ContactActivityTracker Daily Schedule' : jobName;
        String cronTime = '0 0 2 * * ?';
        return System.schedule(jobName, cronTime, new ContactActivityTrackerBatch());
    }

    public void execute(SchedulableContext schedulableContext) {
        Database.executeBatch(new ContactActivityTrackerBatch(), 2000);
    }

    public Database.QueryLocator start(Database.BatchableContext batchableContext) {
        // Both Activity_Indicator__c and Account.Status__c are picklists.
        String query = 'SELECT Id, AccountId, Activity_Indicator__c, Account.Status__c FROM Contact';
        return Database.getQueryLocator(query);
    }

    public void execute(Database.BatchableContext batchableContext, List<Contact> scopeContacts) {
        if (scopeContacts.isEmpty()) {
            return;
        }

        Map<Id, String> indicatorByContactId = new Map<Id, String>();
        Set<Id> contactIdsToProcess = new Set<Id>();
        List<Contact> contactsToUpdate = new List<Contact>();
        Map<Id, Datetime> latestActivityDateByContactId = new Map<Id, Datetime>();
        Date today = Date.today();

        for (Contact contact : scopeContacts) {
            //What if status or account is null?
            if (contact.Account.Status__c == 'Inactive') {
                if (contact.Activity_Indicator__c != '0') {
                    contactsToUpdate.add(new Contact(Id = contact.Id, Activity_Indicator__c = '0'));
                }
                continue;
            }
            contactIdsToProcess.add(contact.Id);
            indicatorByContactId.put(contact.Id, contact.Activity_Indicator__c);
        }

        if (!contactIdsToProcess.isEmpty()) {
            for (AggregateResult aggResult : [
                SELECT WhoId contactId, MAX(LastModifiedDate) latestDate
                FROM Task
                WHERE WhoId IN :contactIdsToProcess
                GROUP BY WhoId
            ]) {
                upsertLatest(
                    latestActivityDateByContactId,
                    (Id) aggResult.get('contactId'),
                    (Datetime) aggResult.get('latestDate')
                );
            }
            for (AggregateResult aggResult : [
                SELECT WhoId contactId, MAX(LastModifiedDate) latestDate
                FROM Event
                WHERE WhoId IN :contactIdsToProcess
                GROUP BY WhoId
            ]) {
                upsertLatest(
                    latestActivityDateByContactId,
                    (Id) aggResult.get('contactId'),
                    (Datetime) aggResult.get('latestDate')
                );
            }
        }

        for (Id contactId : contactIdsToProcess) {
            String indicatorValue;
            if (latestActivityDateByContactId.containsKey(contactId)) {
                Integer daysSince = today.daysBetween(latestActivityDateByContactId.get(contactId).date());
                indicatorValue = classifyIndicatorByDaysSince(Math.abs(daysSince));
            } else {
                indicatorValue = '-1';
            }
            if (indicatorByContactId.get(contactId) != indicatorValue) {
                contactsToUpdate.add(new Contact(Id = contactId, Activity_Indicator__c = indicatorValue));
            }
        }

        if (!contactsToUpdate.isEmpty()) {
            Database.SaveResult[] results = Database.update(contactsToUpdate, false);
            for (Integer i = 0; i < results.size(); i++) {
                if (!results[i].isSuccess()) {
                    failedContactIds.add(contactsToUpdate[i].Id);
                }
            }
        }
    }

    public void finish(Database.BatchableContext batchableContext) {
        sendContactEmail(failedContactIds);
        sendChunkErrorEmail(batchableContext.getJobId());
    }

    @TestVisible
    private static List<Messaging.SendEmailResult> sendContactEmail(List<String> failedIds) {
        if (failedIds.isEmpty()) {
            return null;
        }

        String body = String.format(
            'The ContactActivityTrackerBatch job has completed, but some contacts failed to update.\n\n' +
                'Total failed records: {0}\n\n' +
                'List of failed Contact Ids:\n{1}',
            new List<String>{ String.valueOf(failedIds.size()), String.join(failedIds, '\n') }
        );

        Messaging.SingleEmailMessage mail = new Messaging.SingleEmailMessage();
        mail.setToAddresses(new List<String>{ 'admin@yourcompany.com' });
        mail.setSubject('ContactActivityTrackerBatch Failure Report');
        mail.setPlainTextBody(body);

        return Messaging.sendEmail(new List<Messaging.SingleEmailMessage>{ mail });
    }

    @TestVisible
    private static List<Messaging.SendEmailResult> sendChunkErrorEmail(Id jobId) {
        if (jobId == null) {
            return null;
        }

        AsyncApexJob job = [
            SELECT Id, Status, NumberOfErrors, JobItemsProcessed, TotalJobItems
            FROM AsyncApexJob
            WHERE Id = :jobId
            LIMIT 1
        ];

        if (job.NumberOfErrors == 0) {
            return null;
        }

        String body = String.format(
            'The ContactActivityTrackerBatch job has completed with chunk-level errors.\n\n' +
            'Job Id: {0}\nStatus: {1}\nProcessed: {2}/{3}\nTotal chunk errors: {4}\n',
            new List<String>{
                String.valueOf(job.Id),
                String.valueOf(job.Status),
                String.valueOf(job.JobItemsProcessed),
                String.valueOf(job.TotalJobItems),
                String.valueOf(job.NumberOfErrors)
            }
        );

        Messaging.SingleEmailMessage mail = new Messaging.SingleEmailMessage();
        mail.setToAddresses(new List<String>{ 'admin@yourcompany.com' });
        mail.setSubject('ContactActivityTrackerBatch – Chunk Errors');
        mail.setPlainTextBody(body);

        return Messaging.sendEmail(new List<Messaging.SingleEmailMessage>{ mail });
    }

    private static void upsertLatest(
        Map<Id, Datetime> latestActivityDateByContactId,
        Id contactId,
        Datetime candidateDate
    ) {
        if (contactId == null || candidateDate == null) {
            return;
        }
        if (
            !latestActivityDateByContactId.containsKey(contactId) ||
            latestActivityDateByContactId.get(contactId) < candidateDate
        ) {
            latestActivityDateByContactId.put(contactId, candidateDate);
        }
    }

    private static String classifyIndicatorByDaysSince(Integer daysSince) {
        if (daysSince <= 90) {
            return '1';
        }
        if (daysSince <= 180) {
            return '2';
        }
        if (daysSince <= 270) {
            return '3';
        }
        if (daysSince <= 360) {
            return '4';
        }
        return '5';
    }
}
