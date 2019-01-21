import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class savePullReplicationSinkTaskCommand extends commandBase {

    constructor(private db: database, private replicationSettings: Raven.Client.Documents.Operations.Replication.PullReplicationAsSink) {
        super();
    }

    execute(): JQueryPromise<Raven.Client.Documents.Operations.Replication.PullReplicationAsSink> {
        const url = endpoints.databases.pullReplication.adminTasksSinkPullReplication;
        const payload = {
            PullReplicationAsSink: this.replicationSettings
        };

        return this.post<Raven.Client.Documents.Operations.Replication.PullReplicationAsSink>(url, JSON.stringify(payload), this.db)
            .fail((response: JQueryXHR) => {
                this.reportError("Failed to save pull replication sink task", response.responseText, response.statusText);
            })
            .done(() => {
                this.reportSuccess(`Saved pull replication sink task`);
            });
    }
}

export = savePullReplicationSinkTaskCommand; 

