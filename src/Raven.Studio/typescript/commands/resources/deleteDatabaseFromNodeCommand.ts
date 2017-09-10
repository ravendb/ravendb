import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");
import pluralizeHelpers = require("common/helpers/text/pluralizeHelpers");

class deleteDatabaseFromNodeCommand extends commandBase {

    constructor(private db: database, private nodes: Array<string>, private isHardDelete: boolean) {
        super();
    }

    execute(): JQueryPromise<updateDatabaseConfigurationsResult> {
        const url = endpoints.global.adminDatabases.adminDatabases;
        const args = {
            HardDelete: this.isHardDelete,
            DatabaseNames: [this.db.name],
            FromNodes: this.nodes
        };

        return this.del<updateDatabaseConfigurationsResult>(url, JSON.stringify(args), null)
            .done(() => {
                this.reportSuccess(`Successfully deleted database from ${pluralizeHelpers.pluralize(this.nodes.length, "node", "nodes: ")} ${this.nodes.join(", ")}.`);
            })
            .fail((response: JQueryXHR) => this.reportError("Failed to delete database from nodes: " + this.nodes.join(", "), response.responseText, response.statusText));
    }


} 

export = deleteDatabaseFromNodeCommand;
