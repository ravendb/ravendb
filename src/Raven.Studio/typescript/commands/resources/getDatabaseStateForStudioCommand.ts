import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import StudioDatabaseState = Raven.Server.Web.System.Processors.Studio.StudioDatabasesHandlerForGetDatabasesState.StudioDatabaseState;

class getDatabaseStateForStudioCommand extends commandBase {

    private readonly nodeTag: string;
    private readonly databaseName: string;
    
    constructor(nodeTag: string, databaseName: string) {
        super();
        this.nodeTag = nodeTag;
        this.databaseName = databaseName;
    }
    
    execute(): JQueryPromise<StudioDatabaseState> {
        const url = endpoints.global.studioDatabases.studioTasksDatabasesState;
        const args = {
            nodeTag: this.nodeTag,
            name: this.databaseName
        }

        return this.query<StudioDatabaseState>(url, args, undefined, x => x.Databases[0])
            .fail((response: JQueryXHR) => this.reportError("Failed to load database state", response.responseText, response.statusText));
    }
}

export = getDatabaseStateForStudioCommand;
