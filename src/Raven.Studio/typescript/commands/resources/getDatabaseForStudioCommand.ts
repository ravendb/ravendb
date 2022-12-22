import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class getDatabaseForStudioCommand extends commandBase {

    private readonly dbName: string;

    constructor(dbName: string) {
        super();
        this.dbName = dbName;
    }

    execute(): JQueryPromise<StudioDatabaseResponse> {
        const url = endpoints.global.studioDatabases.studioTasksDatabases;
        const args = {
            name: this.dbName
        };
        
        return this.query(url, args, null, x => x.Databases[0]);
    }
}

export = getDatabaseForStudioCommand;
