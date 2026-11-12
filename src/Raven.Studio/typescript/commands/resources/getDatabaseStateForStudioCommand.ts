import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import StudioDatabasesState = Raven.Server.Web.System.Processors.Studio.StudioDatabasesHandlerForGetDatabasesState.StudioDatabasesState;

class getDatabaseStateForStudioCommand extends commandBase {
    private readonly nodeTag: string;
    private readonly databaseName: string;

    protected readonly showProtractedRequestMessage = false;

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

        return this.query<StudioDatabasesState>(url, args, null, null, { timeout: commandBase.perNodeRequestTimeoutMs });
    }
}

export = getDatabaseStateForStudioCommand;
