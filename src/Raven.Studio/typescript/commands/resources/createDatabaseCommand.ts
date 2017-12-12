import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class createDatabaseCommand extends commandBase {

    constructor(private databaseDocument: Raven.Client.ServerWide.DatabaseRecord, private replicationFactor: number) {
        super();
    }

    execute(): JQueryPromise<Raven.Client.ServerWide.Operations.DatabasePutResult> {
        const args = {
            name: this.databaseDocument.DatabaseName,
            replicationFactor: this.replicationFactor
        };
        const url = endpoints.global.adminDatabases.adminDatabases + this.urlEncodeArgs(args);
        return this.put<Raven.Client.ServerWide.Operations.DatabasePutResult>(url, JSON.stringify(this.databaseDocument), null, { dataType: undefined })
            .done(() => this.reportSuccess(this.databaseDocument.DatabaseName + " created"))
            .fail((response: JQueryXHR) => this.reportError("Failed to create database", response.responseText, response.statusText));
    }
}

export = createDatabaseCommand; 
