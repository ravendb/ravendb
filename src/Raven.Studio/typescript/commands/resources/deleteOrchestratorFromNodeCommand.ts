import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import { DatabaseSharedInfo } from "components/models/databases";

class deleteOrchestratorFromNodeCommand extends commandBase {

    private db: DatabaseSharedInfo;

    private node: string;

    constructor(db: DatabaseSharedInfo, node: string) {
        super();
        this.node = node;
        this.db = db;
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
