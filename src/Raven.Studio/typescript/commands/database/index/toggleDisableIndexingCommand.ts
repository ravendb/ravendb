import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import { DatabaseSharedInfo } from "components/models/databases";

class toggleDisableIndexingCommand extends commandBase {

    private readonly start: boolean;

    private readonly db: DatabaseSharedInfo;

    constructor(start: boolean, db: DatabaseSharedInfo) {
        super();
        this.db = db;
        this.start = start;
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
