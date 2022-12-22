import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class addOrchestratorToDatabaseGroupCommand extends commandBase {

    constructor(private databaseName: string, private nodeTagToAdd: string) {
        super();
    }

    execute(): JQueryPromise<Raven.Client.ServerWide.Operations.DatabasePutResult> {
        const args = {
            name: this.databaseName,
            node: this.nodeTagToAdd,
        };
        const url = endpoints.global.shardedAdminDatabase.adminDatabasesOrchestrator + this.urlEncodeArgs(args);

        return this.put<Raven.Client.ServerWide.Operations.DatabasePutResult>(url, null)
            .done(() => this.reportSuccess("Node " + this.nodeTagToAdd + " was added as orchestrator to " + this.databaseName))
            .fail((response: JQueryXHR) => this.reportError("Failed to add orchestrator to database group", response.responseText, response.statusText));
    }
}

export = addOrchestratorToDatabaseGroupCommand;
