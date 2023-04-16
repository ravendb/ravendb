import commandBase = require("commands/commandBase");
import databaseInfo = require("models/resources/info/databaseInfo");
import endpoints = require("endpoints");

class restartDatabaseCommand extends commandBase {

    constructor(private db: databaseInfo) {
        super();
    }

    execute(): JQueryPromise<statusDto<disableDatabaseResult>> {
        const args = {
            name: this.db.name
        };

        const url = endpoints.global.adminDatabases.adminDatabasesRestart + this.urlEncodeArgs(args);

        return this.post(url, null, null, { dataType: undefined })
            .fail((response: JQueryXHR) => this.reportError("Failed to restart the database", response.responseText, response.statusText));
    }

}

export = restartDatabaseCommand;  
