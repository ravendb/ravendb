import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class dropSubscriptionConnectionCommand extends commandBase {

    private readonly db: database;

    private readonly taskId: number;

    private readonly taskName: string;
    
    private readonly nodeTag: string;

    private readonly workerId: string = undefined;

    constructor(db: database, taskId: number, taskName: string, nodeTag: string = undefined, workerId: string = undefined) {
        super();
        this.workerId = workerId;
        this.taskName = taskName;
        this.nodeTag = nodeTag;
        this.taskId = taskId;
        this.db = db;
    }
 
    execute(): JQueryPromise<void> {
        return this.dropSubscription()
            .fail((response: JQueryXHR) => { this.reportError(`Failed to drop subscription: ${this.taskName}`, response.responseText, response.statusText); })
            .done(() => {
                if (this.workerId) {
                    this.reportSuccess(`Subscription connection was dropped successfully`);
                } else {
                    this.reportSuccess(`Subscription ${this.taskName} was dropped successfully`);
                }
            });
    }

    private dropSubscription(): JQueryPromise<void> {
        const args = { 
            id: this.taskId,
            workerId: this.workerId,
            nodeTag: this.nodeTag
        };
        const url = endpoints.databases.subscriptions.subscriptionsDrop + this.urlEncodeArgs(args);

        return this.post(url, null, this.db); 
    }
}

export = dropSubscriptionConnectionCommand; 

