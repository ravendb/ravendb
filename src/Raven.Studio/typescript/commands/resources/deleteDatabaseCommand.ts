import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class deleteDatabaseCommand extends commandBase {

    constructor(private databases: Array<database>, private isHardDelete: boolean) {
        super();
    }

    execute(): JQueryPromise<updateDatabaseConfigurationsResult> {
        const url = endpoints.global.adminDatabases.adminDatabases;
        const args = {
            "hard-delete": this.isHardDelete,
            name: this.databases.map(x => x.name)
        };

        return this.del<updateDatabaseConfigurationsResult>(url + this.urlEncodeArgs(args), null, null, 9000 * this.databases.length)
            .fail((response: JQueryXHR) => this.reportError("Failed to delete databases", response.responseText, response.statusText));
    }


} 

export = deleteDatabaseCommand;
