import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class saveSubscriptionTaskCommand extends commandBase {

    constructor(private db: database, private subscriptionSettings: subscriptionDataFromUI, private taskId?: number, private disabled?: Raven.Client.Server.Operations.OngoingTaskState) {
        super();
    }

    execute(): JQueryPromise<Raven.Client.Server.Operations.ModifyOngoingTaskResult> {
        return this.updateSubscription()
            .fail((response: JQueryXHR) => {
                if (this.taskId) {
                    // update operation
                    this.reportError("Failed to update subscription task", response.responseText, response.statusText);
                } else {
                    // create operation
                    this.reportError("Failed to create subscription task: " + this.subscriptionSettings.TaskName, response.responseText, response.statusText); 
                }
            })
            .done(() => {
                if (this.taskId) {
                    // update operation
                    this.reportSuccess(`Updated subscription task`);
                } else {
                    // create operation
                    this.reportSuccess(`Created subscription task ${this.subscriptionSettings.TaskName} from database ${this.db.name}`);
                }
            });
    }

    private updateSubscription(): JQueryPromise<Raven.Client.Server.Operations.ModifyOngoingTaskResult> {
        let args: any;

        if (this.taskId) { 
            // An existing task
            args = this.disabled === "Disabled" ? { name: this.db.name, id: this.taskId, disabled: true } :
                                                  { name: this.db.name, id: this.taskId };
        } else {
            // New task
            args = { name: this.db.name };
        }
        
        const url = endpoints.databases.subscriptions.subscriptions + this.urlEncodeArgs(args);

        const saveTask = $.Deferred<Raven.Client.Server.Operations.ModifyOngoingTaskResult>();

        const subscriptionToSend: Raven.Client.Documents.Subscriptions.SubscriptionCreationOptions = {
            ChangeVector: this.subscriptionSettings.ChangeVectorEntry,
            Name: this.subscriptionSettings.TaskName,
            Criteria: {
                Collection: this.subscriptionSettings.Collection,
                Script: this.subscriptionSettings.Script,
                IncludeRevisions: this.subscriptionSettings.IncludeRevisions
            }
        };

        this.put(url, JSON.stringify(subscriptionToSend), this.db)
            .done((results: Raven.Client.Server.Operations.ModifyOngoingTaskResult) => { 
                saveTask.resolve(results);
            })
            .fail(response => saveTask.reject(response));

        return saveTask;
    }
}

export = saveSubscriptionTaskCommand; 

