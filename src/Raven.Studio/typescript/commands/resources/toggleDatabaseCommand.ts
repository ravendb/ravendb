import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import { DatabaseSharedInfo } from "components/models/databases";

class toggleDatabaseCommand extends commandBase {

    private readonly dbs: DatabaseSharedInfo[];

    private readonly enable: boolean;

    constructor(dbs: DatabaseSharedInfo[], enable: boolean) {
        super();
        this.enable = enable;
        this.dbs = dbs;
    }

    get action() {
        return this.enable ? "enable" : "disable";
    }

    execute(): JQueryPromise<statusDto<disableDatabaseResult>> {
        const payload: Raven.Client.ServerWide.Operations.ToggleDatabasesStateOperation.Parameters = {
            DatabaseNames: this.dbs.map(x => x.name)
        };

        const url = this.enable ?
            endpoints.global.adminDatabases.adminDatabasesEnable :
            endpoints.global.adminDatabases.adminDatabasesDisable;

        return this.post(url, JSON.stringify(payload))
            .fail((response: JQueryXHR) => this.reportError("Failed to toggle database status", response.responseText, response.statusText));
    }

}

export = toggleDatabaseCommand;  
