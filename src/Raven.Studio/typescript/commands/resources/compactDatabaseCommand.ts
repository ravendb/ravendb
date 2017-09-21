import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");
import databaseInfo = require("models/resources/info/databaseInfo");

class compactDatabaseCommand extends commandBase {

    constructor(private databaseName: string) {
        super();
    }

    execute(): JQueryPromise<operationIdDto> {
        const args = {
            name: this.databaseName
        };

        const url = endpoints.global.adminDatabases.adminCompact + this.urlEncodeArgs(args);
        
        return this.post<operationIdDto>(url, null)
            .fail((response: JQueryXHR) => this.reportError("Failed to compact database", response.responseText, response.statusText));
    }


} 

export = compactDatabaseCommand;
