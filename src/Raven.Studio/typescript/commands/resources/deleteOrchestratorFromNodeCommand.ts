import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");
import pluralizeHelpers = require("common/helpers/text/pluralizeHelpers");

class deleteOrchestratorFromNodeCommand extends commandBase {

    constructor(private db: database, private node: string) {
        super();
    }

    execute(): JQueryPromise<updateDatabaseConfigurationsResult> {
        const url = endpoints.global.shardedAdminDatabase.adminDatabasesOrchestrator;
        const args = {
            name: [this.db.name],
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
