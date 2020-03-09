import commandBase = require("commands/commandBase");
import databaseInfo = require("models/resources/info/databaseInfo");
import endpoints = require("endpoints");

class toggleDisableIndexingCommand extends commandBase {

    constructor(private start: boolean, private db: databaseInfo) {
        super();
    }

    execute(): JQueryPromise<void> {
        const args = {
            disable: !this.start
        }

        const url = endpoints.global.adminDatabases.adminDatabasesToggleIndexing + this.urlEncodeArgs(args);

        const payload = {
            DatabaseNames: [this.db.name]
        } as Raven.Client.ServerWide.Operations.ToggleDatabasesStateOperation.Parameters;

        return this.post(url, JSON.stringify(payload))
            .done(() => {
                const state = this.start ? "Enabled" : "Disabled";
                this.reportSuccess(`Indexing is ${state}`);
            }).fail((response: JQueryXHR) => this.reportError("Failed to toggle indexing status", response.responseText));
    }
}

export = toggleDisableIndexingCommand;
