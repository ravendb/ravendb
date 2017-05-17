import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class disableDatabaseToggleCommand extends commandBase {

    constructor(private dbs: Array<database>, private disable: boolean) {
        super();
    }

    get action() {
        return this.disable ? "disable" : "enable";
    }

    execute(): JQueryPromise<statusDto<disableDatabaseResult>> {
        const args = {
            name: this.dbs.map(x => x.name)
        };

        const endPoint = this.disable ?
            endpoints.global.adminDatabases.adminDatabasesDisable :
            endpoints.global.adminDatabases.adminDatabasesEnable;

        const url = endPoint + this.urlEncodeArgs(args);

        return this.post(url, null)
            .fail((response: JQueryXHR) => this.reportError("Failed to toggle database status", response.responseText, response.statusText));
    }

}

export = disableDatabaseToggleCommand;  
