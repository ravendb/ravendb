import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class saveDatabaseLockModeCommand extends commandBase {

    constructor(private dbs: Array<database>, private lockMode: Raven.Client.ServerWide.DatabaseLockMode) {
        super();
    }

    execute(): JQueryPromise<void> {
        const payload = {
            DatabaseNames: this.dbs.map(x => x.name),
            Mode: this.lockMode
        } as Raven.Client.ServerWide.Operations.ToggleDatabasesStateOperation.Parameters;

        const url = endpoints.global.adminDatabases.adminDatabasesSetLock;

        return this.post(url, JSON.stringify(payload), null, { dataType: undefined })
            .fail((response: JQueryXHR) => this.reportError("Failed to toggle database lock mode", response.responseText, response.statusText));
    }

}

export = saveDatabaseLockModeCommand;  
