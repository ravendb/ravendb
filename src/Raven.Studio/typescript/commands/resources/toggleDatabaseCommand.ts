import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class toggleDatabaseCommand extends commandBase {

    private readonly databaseNames: string[];

    private readonly enable: boolean;

    constructor(databaseNames: string[], enable: boolean) {
        super();
        this.enable = enable;
        this.databaseNames = databaseNames;
    }

    get action() {
        return this.enable ? "enable" : "disable";
    }

    execute(): JQueryPromise<statusDto<disableDatabaseResult>> {
        const payload: Raven.Client.ServerWide.Operations.ToggleDatabasesStateOperation.Parameters = {
            DatabaseNames: this.databaseNames
        };

        const url = this.enable ?
            endpoints.global.adminDatabases.adminDatabasesEnable :
            endpoints.global.adminDatabases.adminDatabasesDisable;

        return this.post(url, JSON.stringify(payload))
            .fail((response: JQueryXHR) => this.reportError("Failed to toggle database status", response.responseText, response.statusText));
    }

}

export = toggleDatabaseCommand;  
