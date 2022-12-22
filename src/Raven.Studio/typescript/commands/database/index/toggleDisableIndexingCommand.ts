import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class toggleDisableIndexingCommand extends commandBase {

    constructor(private start: boolean, private db: {  name: string }) { //TODO:
        super();
    }

    execute(): JQueryPromise<void> {
        const args = {
            enable: this.start
        }

        const url = endpoints.global.adminDatabases.adminDatabasesIndexing + this.urlEncodeArgs(args);

        const payload: Raven.Client.ServerWide.Operations.ToggleDatabasesStateOperation.Parameters = {
            DatabaseNames: [this.db.name]
        };

        //TODO: report messages!
        return this.post(url, JSON.stringify(payload));
    }
}

export = toggleDisableIndexingCommand;
