import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class createDatabaseCommand extends commandBase {

    constructor(private databaseDocument: Raven.Client.Server.DatabaseRecord, private replicationFactor: number) {
        super();
    }

    execute(): JQueryPromise<Raven.Server.Web.System.DatabasePutResult> {
        const args = {
            name: this.databaseDocument.DatabaseName,
            'replication-factor': this.replicationFactor
        };
        const url = endpoints.global.adminDatabases.adminDatabases + this.urlEncodeArgs(args);
        return this.put(url, JSON.stringify(this.databaseDocument), null, { dataType: undefined })
            .done(() => this.reportSuccess(this.databaseDocument.DatabaseName + " created"))
            .fail((response: JQueryXHR) => this.reportError("Failed to create database", response.responseText, response.statusText));
    }
}

export = createDatabaseCommand; 
