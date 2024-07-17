import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class saveDatabaseLockModeCommand extends commandBase {

    private dbs: Array<database | string>;

    private lockMode: Raven.Client.ServerWide.DatabaseLockMode;

    constructor(dbs: Array<database | string>, lockMode: Raven.Client.ServerWide.DatabaseLockMode) {
        super();
        this.lockMode = lockMode;
        this.dbs = dbs;
    }

    execute(): JQueryPromise<void> {
        const payload: Raven.Client.ServerWide.Operations.SetDatabasesLockOperation.Parameters = {
            DatabaseNames: this.dbs.map(x => (typeof x === "string" ? x : x.name)),
            Mode: this.lockMode
        };

        const url = endpoints.global.adminDatabases.adminDatabasesSetLock;

        return this.post(url, JSON.stringify(payload), null, { dataType: undefined })
            .fail((response: JQueryXHR) => this.reportError("Failed to toggle database lock mode", response.responseText, response.statusText));
    }

}

export = saveDatabaseLockModeCommand;  
