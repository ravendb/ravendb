import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class saveSubscriptionTaskCommand extends commandBase {
    private subscriptionToSend: Raven.Client.Documents.Subscriptions.SubscriptionCreationOptions; 

    constructor(private db: database, private subscriptionSettings: subscriptionDataFromUI, private taskId?: number, private disabled?: Raven.Client.Server.Operations.OngoingTaskState) {
        super();
      
        this.subscriptionToSend = {
            ChangeVector: this.subscriptionSettings.ChangeVectorEntry,
            Name: subscriptionSettings.TaskName,
            Criteria: {
                Collection: this.subscriptionSettings.Collection,
                Script: this.subscriptionSettings.Script,
                IsVersioned: this.subscriptionSettings.IsVersioned
            }
        };
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

        const addRepTask = $.Deferred<Raven.Client.Server.Operations.ModifyOngoingTaskResult>();

        const payload = this.subscriptionToSend;

        this.put(url, JSON.stringify(payload), this.db)
            .done((results: Array<Raven.Client.Server.Operations.ModifyOngoingTaskResult>) => { 
                addRepTask.resolve(results[0]);
            })
            .fail(response => addRepTask.reject(response));

        return addRepTask;
    }
}

export = saveSubscriptionTaskCommand; 

