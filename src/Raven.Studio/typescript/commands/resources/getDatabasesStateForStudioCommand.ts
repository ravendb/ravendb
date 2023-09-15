import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import StudioDatabasesState = Raven.Server.Web.System.Processors.Studio.StudioDatabasesHandlerForGetDatabasesState.StudioDatabasesState;

class getDatabasesStateForStudioCommand extends commandBase {

    private readonly nodeTag: string;
    
    constructor(nodeTag: string) {
        super();
        this.nodeTag = nodeTag;
    }
    
    execute(): JQueryPromise<StudioDatabasesState> {
        const url = endpoints.global.studioDatabases.studioTasksDatabasesState;
        const args = {
            nodeTag: this.nodeTag
        }

        return this.query<StudioDatabasesResponse>(url, args);
    }
}

export = getDatabasesStateForStudioCommand;
