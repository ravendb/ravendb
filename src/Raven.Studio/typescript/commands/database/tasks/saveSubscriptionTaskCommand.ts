import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class saveSubscriptionTaskCommand extends commandBase {
    private readonly databaseName: string;
    private readonly taskId: number;
    private readonly payload: Raven.Client.Documents.Subscriptions.SubscriptionCreationOptions;

    constructor( db: database | string, payload: Raven.Client.Documents.Subscriptions.SubscriptionCreationOptions,  taskId?: number) {
        super();
        this.databaseName = (typeof db === "string" ? db : db.name);
        this.taskId = taskId;
        this.payload = payload;
    }

    execute(): JQueryPromise<Raven.Client.Documents.Operations.OngoingTasks.ModifyOngoingTaskResult> {
        return this.updateSubscription()
            .fail((response: JQueryXHR) => {
                this.reportError("Failed to save subscription task", response.responseText, response.statusText); 
            })
            .done(() => {
                this.reportSuccess(`Saved subscription task ${this.payload.Name} from database ${this.databaseName}`);
            });
    }

    private updateSubscription(): JQueryPromise<Raven.Client.Documents.Operations.OngoingTasks.ModifyOngoingTaskResult> {
        let args: any;

        if (this.taskId) {
            args = { name: this.databaseName, id: this.taskId };
        } else {
            // New task
            args = { name: this.databaseName };
        }
        
        const url = endpoints.databases.subscriptions.subscriptions + this.urlEncodeArgs(args);

        const saveTask = $.Deferred<Raven.Client.Documents.Operations.OngoingTasks.ModifyOngoingTaskResult>();

        this.put(url, JSON.stringify(this.payload), this.databaseName)
            .done((results: Raven.Client.Documents.Operations.OngoingTasks.ModifyOngoingTaskResult) => { 
                saveTask.resolve(results);
            })
            .fail(response => saveTask.reject(response));

        return saveTask;
    }
}

export = saveSubscriptionTaskCommand; 

