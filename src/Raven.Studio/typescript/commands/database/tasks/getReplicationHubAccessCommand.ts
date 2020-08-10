import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class getReplicationHubAccessCommand extends commandBase {

    constructor(private db: database, private hubTaskName: string) {
          super();
    }

    execute(): JQueryPromise<Raven.Client.Documents.Operations.Replication.ReplicationHubAccessResult> {
        const args = {
            name: this.hubTaskName
        };
        
        const url = endpoints.databases.pullReplication.adminTasksPullReplicationHubAccess + this.urlEncodeArgs(args);
        
        return this.query<Raven.Client.Documents.Operations.Replication.ReplicationHubAccessResult>(url, null, this.db)
            .fail((response: JQueryXHR) => {
                this.reportError(`Failed to get Replication Access details for Hub task: ${this.hubTaskName}. `, response.responseText, response.statusText);
            });
    }
}

export = getReplicationHubAccessCommand; 
