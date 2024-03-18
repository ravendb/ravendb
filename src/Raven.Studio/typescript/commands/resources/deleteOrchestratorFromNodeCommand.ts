import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class deleteOrchestratorFromNodeCommand extends commandBase {

    private databaseName: string;

    private node: string;

    constructor(databaseName: string, node: string) {
        super();
        this.node = node;
        this.databaseName = databaseName;
    }

    execute(): JQueryPromise<updateDatabaseConfigurationsResult> {
        const url = endpoints.global.shardedAdminDatabase.adminDatabasesOrchestrator;
        const args = {
            name: [this.databaseName],
            node: this.node
        };

        return this.del<updateDatabaseConfigurationsResult>(url + this.urlEncodeArgs(args), null)
            .done(() => {
                this.reportSuccess(`Successfully deleted orchestrator from node ${this.node}.`);
            })
            .fail((response: JQueryXHR) => this.reportError("Failed to delete orchestrator from node: " + this.node, response.responseText, response.statusText));
    }


} 

export = deleteOrchestratorFromNodeCommand;
