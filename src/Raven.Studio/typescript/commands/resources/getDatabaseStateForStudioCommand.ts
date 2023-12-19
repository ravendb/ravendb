import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import StudioDatabasesState = Raven.Server.Web.System.Processors.Studio.StudioDatabasesHandlerForGetDatabasesState.StudioDatabasesState;

class getDatabaseStateForStudioCommand extends commandBase {
    private readonly nodeTag: string;
    private readonly databaseName: string;

    constructor(nodeTag: string, databaseName: string) {
        super();
        this.nodeTag = nodeTag;
        this.databaseName = databaseName;
    }

    execute(): JQueryPromise<StudioDatabasesState> {
        const url = endpoints.global.studioDatabases.studioTasksDatabasesState;
        const args = {
            nodeTag: this.nodeTag,
            name: this.databaseName,
        };

        return this.query<StudioDatabasesState>(url, args, undefined);
    }
}

export = getDatabaseStateForStudioCommand;
