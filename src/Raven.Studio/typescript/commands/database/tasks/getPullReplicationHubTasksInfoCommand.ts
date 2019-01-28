import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class getPullReplicationHubTasksInfoCommand extends commandBase {

    constructor(private db: database, private taskId: number) {
          super();
    }

    execute(): JQueryPromise<Raven.Client.Documents.Operations.Replication.PullReplicationDefinitionAndCurrentConnections> {
        const args = {
            key: this.taskId
        };
        
        const url = endpoints.databases.ongoingTasks.tasksPullReplicationHub + this.urlEncodeArgs(args);
        
        return this.query<Raven.Client.Documents.Operations.Replication.PullReplicationDefinitionAndCurrentConnections>(url, null, this.db)
            .fail((response: JQueryXHR) => {
                this.reportError(`Failed to get info for pull replication hub task with id: ${this.taskId}. `, response.responseText, response.statusText);    
            });
    }

}

export = getPullReplicationHubTasksInfoCommand; 
