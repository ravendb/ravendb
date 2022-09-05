import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class deleteDatabaseCommand extends commandBase {

    constructor(private databases: string[], private isHardDelete: boolean) {
        super();
    }

    execute(): JQueryPromise<updateDatabaseConfigurationsResult> {
        const url = endpoints.global.adminDatabases.adminDatabases;
        
        const payload: Raven.Client.ServerWide.Operations.DeleteDatabasesOperation.Parameters = {
            HardDelete: this.isHardDelete,
            DatabaseNames: this.databases,
            FromNodes: undefined
        };

        return this.del<updateDatabaseConfigurationsResult>(url, JSON.stringify(payload), null, null, 9000 * this.databases.length)
            .fail((response: JQueryXHR) => this.reportError("Failed to delete databases", response.responseText, response.statusText));
    }


} 

export = deleteDatabaseCommand;
