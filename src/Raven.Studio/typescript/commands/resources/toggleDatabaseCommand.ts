import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class toggleDatabaseCommand extends commandBase {

    constructor(private dbs: Array<database>, private disable: boolean) {
        super();
    }

    get action() {
        return this.disable ? "disable" : "enable";
    }

    execute(): JQueryPromise<statusDto<disableDatabaseResult>> {
        const payload = {
            DatabaseNames: this.dbs.map(x => x.name)
        } as Raven.Client.ServerWide.Operations.ToggleDatabasesStateOperation.Parameters;

        const url = this.disable ?
            endpoints.global.adminDatabases.adminDatabasesDisable :
            endpoints.global.adminDatabases.adminDatabasesEnable;

        return this.post(url, JSON.stringify(payload))
            .fail((response: JQueryXHR) => this.reportError("Failed to toggle database status", response.responseText, response.statusText));
    }

}

export = toggleDatabaseCommand;  
