import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class dropSubscriptionConnectionCommand extends commandBase {

    constructor(private db: database, private taskId: number, private taskName: string, private connectionId: number = undefined) {
        super();
    }
 
    execute(): JQueryPromise<void> {
        return this.dropSubscription()
            .fail((response: JQueryXHR) => { this.reportError(`Failed to drop subscription: ${this.taskName}`, response.responseText, response.statusText); })
            .done(() => {
                if (this.connectionId) {
                    this.reportSuccess(`Subscription connection was dropped successfully`);
                } else {
                    this.reportSuccess(`Subscription ${this.taskName} was dropped successfully`);
                }
            });
    }

    private dropSubscription(): JQueryPromise<void> {
        const args = { 
            id: this.taskId,
            connectionId: this.connectionId
        };
        const url = endpoints.databases.subscriptions.subscriptionsDrop + this.urlEncodeArgs( args );

        return this.post(url, null, this.db); 
    }
}

export = dropSubscriptionConnectionCommand; 

