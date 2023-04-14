import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class getSubscriptionConnectionDetailsCommand extends commandBase {

    private readonly db: database;

    private readonly taskId: number;

    private readonly taskName: string;

    private readonly nodeTag?: string;

    constructor(db: database, taskId: number, taskName: string, nodeTag?: string) {
        super();
        this.nodeTag = nodeTag;
        this.taskName = taskName;
        this.taskId = taskId;
        this.db = db;
    }
    
    execute(): JQueryPromise<Raven.Server.Documents.TcpHandlers.SubscriptionConnectionsDetails> {
        return this.getConnectionDetails()
            .fail((response: JQueryXHR) => {
                this.reportError(`Failed to get connection details`, response.responseText, response.statusText);
            });
    }

    private getConnectionDetails(): JQueryPromise<Raven.Server.Documents.TcpHandlers.SubscriptionConnectionsDetails> {
        const url = endpoints.databases.subscriptions.subscriptionsConnectionDetails;
        const args = { id: this.taskId, name: this.taskName, nodeTag: this.nodeTag };
        
        // Note: The 'relative url' has to be prefixed with the 'base url' 
        //       because the connection info (held by the responsible node) might Not be in the server that is on the current browser location
        return this.query<any>(url, args, this.db, null, null, 9000);
    }
}

export = getSubscriptionConnectionDetailsCommand; 
