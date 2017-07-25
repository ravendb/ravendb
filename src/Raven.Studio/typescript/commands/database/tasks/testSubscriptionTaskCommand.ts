import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");
import document = require("models/database/documents/document");

class testSubscriptionTaskCommand extends commandBase {

    constructor(private db: database, private subscriptionSettings: subscriptionDataFromUI, private resultsLimit: number) {
        super();
    }

    execute(): JQueryPromise<pagedResult<document>> {
        return this.testSubscription()
            .fail((response: JQueryXHR) => {
                this.reportError("Failed to test subscription task", response.responseText, response.statusText);
            });
    }

    private testSubscription(): JQueryPromise<pagedResult<document>> {
        
        const args = { pageSize: this.resultsLimit }; 

        const url = endpoints.databases.subscriptions.subscriptionsTry + this.urlEncodeArgs(args);

        const testTask = $.Deferred<pagedResult<document>>();

        const subscriptionToTest: Raven.Client.Documents.Subscriptions.SubscriptionTryout = {
            ChangeVector: this.subscriptionSettings.ChangeVectorEntry,
            Collection: this.subscriptionSettings.Collection,
            Script: this.subscriptionSettings.Script,
            IncludeRevisions: this.subscriptionSettings.IncludeRevisions
        };

        this.post(url, JSON.stringify(subscriptionToTest), this.db)
            .done((dto: resultsDto<documentDto>) => { 

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
                    
                    totalResultCount: dto.Results.length
                } as pagedResult<document>;

                testTask.resolve(result);
            })
            .fail(response => testTask.reject(response));

        return testTask;
    }
}

export = testSubscriptionTaskCommand; 

