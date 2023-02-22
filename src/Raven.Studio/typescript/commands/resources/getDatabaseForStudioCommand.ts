import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import StudioDatabaseInfo = Raven.Server.Web.System.Processors.Studio.StudioDatabasesHandlerForGetDatabases.StudioDatabaseInfo;

class getDatabaseForStudioCommand extends commandBase {

    private readonly dbName: string;

    constructor(dbName: string) {
        super();
        this.dbName = dbName;
    }

    execute(): JQueryPromise<StudioDatabaseInfo> {
        const url = endpoints.global.studioDatabases.studioTasksDatabases;
        const args = {
            name: this.dbName
        };
        
        return this.query(url, args, null, x => x.Databases[0]);
    }
}

export = getDatabaseForStudioCommand;
