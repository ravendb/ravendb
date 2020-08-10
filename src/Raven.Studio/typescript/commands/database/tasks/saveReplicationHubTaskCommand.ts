import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class saveReplicationHubTaskCommand extends commandBase {

    constructor(private db: database, private replicationSettings: Raven.Client.Documents.Operations.Replication.PullReplicationDefinition) {
        super();
    }

    execute(): JQueryPromise<Raven.Client.Documents.Operations.OngoingTasks.ModifyOngoingTaskResult> {
        const url = endpoints.databases.pullReplication.adminTasksPullReplicationHub;

        return this.put<Raven.Client.Documents.Operations.OngoingTasks.ModifyOngoingTaskResult>(url, JSON.stringify(this.replicationSettings), this.db)
            .fail((response: JQueryXHR) => {
                this.reportError("Failed to save the Replication Hub task", response.responseText, response.statusText);
            })
            .done(() => {
                this.reportSuccess(`Replication Hub task was saved successfully`);
            });
    }
}

export = saveReplicationHubTaskCommand; 

