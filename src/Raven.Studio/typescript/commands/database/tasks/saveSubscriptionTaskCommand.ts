import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class saveSubscriptionTaskCommand extends commandBase {

    constructor(private db: database, private payload: Raven.Client.Documents.Subscriptions.SubscriptionCreationOptions, private taskId?: number, private disabled?: Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskState) {
        super();
    }

    execute(): JQueryPromise<Raven.Client.Documents.Operations.OngoingTasks.ModifyOngoingTaskResult> {
        return this.updateSubscription()
            .fail((response: JQueryXHR) => {
                this.reportError("Failed to save subscription task", response.responseText, response.statusText); 
            })
            .done(() => {
                this.reportSuccess(`Saved subscription task ${this.payload.Name} from database ${this.db.name}`);
            });
    }

    private updateSubscription(): JQueryPromise<Raven.Client.Documents.Operations.OngoingTasks.ModifyOngoingTaskResult> {
        let args: any;

        if (this.taskId) { 
            args = { name: this.db.name, id: this.taskId, disabled: this.disabled === "Disabled" };
        } else {
            // New task
            args = { name: this.db.name };
        }
        
        const url = endpoints.databases.subscriptions.subscriptions + this.urlEncodeArgs(args);

        const saveTask = $.Deferred<Raven.Client.Documents.Operations.OngoingTasks.ModifyOngoingTaskResult>();

        this.put(url, JSON.stringify(this.payload), this.db)
            .done((results: Raven.Client.Documents.Operations.OngoingTasks.ModifyOngoingTaskResult) => { 
                saveTask.resolve(results);
            })
            .fail(response => saveTask.reject(response));

        return saveTask;
    }
}

export = saveSubscriptionTaskCommand; 

