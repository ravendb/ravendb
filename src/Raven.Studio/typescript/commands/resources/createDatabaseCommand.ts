import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class createDatabaseCommand extends commandBase {

    constructor(private databaseDocument: Raven.Client.Documents.DatabaseRecord) {
        super();
    }

    execute(): JQueryPromise<any> {
        const args = {
            name: this.databaseDocument.DatabaseName
        };
        const url = endpoints.global.adminDatabases.adminDatabases + this.urlEncodeArgs(args);
        return this.put(url, JSON.stringify(this.databaseDocument), null, { dataType: undefined })
            .done(() => this.reportSuccess(this.databaseDocument.DatabaseName + " created"))
            .fail((response: JQueryXHR) => this.reportError("Failed to create database", response.responseText, response.statusText));
    }
}

export = createDatabaseCommand; 
