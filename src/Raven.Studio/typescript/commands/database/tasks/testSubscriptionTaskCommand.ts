import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");
import document = require("models/database/documents/document");

class testSubscriptionTaskCommand extends commandBase {

    constructor(private db: database, private payload: Raven.Client.Documents.Subscriptions.SubscriptionTryout, private resultsLimit: number, private timeLimit: number) {
        super();
    }

    execute(): JQueryPromise<testSubscriptionPagedResult<document>> {
        return this.testSubscription()
            .fail((response: JQueryXHR) => {
                this.reportError("Failed to test subscription task", response.responseText, response.statusText);
            });
    }

    private testSubscription(): JQueryPromise<testSubscriptionPagedResult<document>> {
        
        const args = { pageSize: this.resultsLimit, timeLimit: this.timeLimit }; 

        const url = endpoints.databases.subscriptions.subscriptionsTry + this.urlEncodeArgs(args);

        const testTask = $.Deferred<testSubscriptionPagedResult<document>>();

        this.post(url, JSON.stringify(this.payload), this.db)
            .done((dto: queryResultDto<documentDto>) => { 

                const result = {

                    items: dto.Results.map((x: document | Raven.Server.Documents.Handlers.DocumentWithException) => {
                        if ('@metadata' in x) {
                            // ==> plain document
                            return new document(x); 
                        } else {
                            // ==> document with exception 
                            const ex = x as Raven.Server.Documents.Handlers.DocumentWithException;
                            ex.DocumentData["@metadata"]["@id"] = ex.Id;

                            const doc = new document(ex.DocumentData);
                            (doc as any).Exception = ex.Exception;
                            (doc as any).ChangeVector = ex.ChangeVector;

                            return doc;
                        }
                    }), 
                    includes: dto.Includes,
                    totalResultCount: dto.Results.length,
                    
                } as testSubscriptionPagedResult<document>;

                testTask.resolve(result);
            })
            .fail(response => testTask.reject(response));

        return testTask;
    }
}

export = testSubscriptionTaskCommand; 

