import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import StudioDatabasesState = Raven.Server.Web.System.Processors.Studio.StudioDatabasesHandlerForGetDatabasesState.StudioDatabasesState;

class getDatabasesStateForStudioCommand extends commandBase {

    private readonly nodeTag: string;

    protected readonly showProtractedRequestMessage = false;

    constructor(nodeTag: string) {
        super();
        this.nodeTag = nodeTag;
    }
    
    execute(): JQueryPromise<StudioDatabasesState> {
        const url = endpoints.global.studioDatabases.studioTasksDatabasesState;
        const args = {
            nodeTag: this.nodeTag
        }

        return this.query<StudioDatabasesResponse>(url, args, null, null, { timeout: commandBase.perNodeRequestTimeoutMs });
    }
}

export = getDatabasesStateForStudioCommand;
