import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class getDatabaseForStudioCommand extends commandBase {

    private readonly dbName: string;

    constructor(dbName: string) {
        super();
        this.dbName = dbName;
    }

    execute(): JQueryPromise<StudioDatabaseResponse> {
        const deferred = $.Deferred<StudioDatabaseResponse>();
        
        const url = endpoints.global.studioDatabases.studioTasksDatabases;
        const args = {
            name: this.dbName
        };
        
        this.query(url, args)
            .done((result: any) => {
                deferred.resolve(result.Databases[0]);
            })
            .fail((xhr: JQueryXHR) => {
                if (xhr.status === 404) {
                    deferred.resolve(null);
                } else {
                    deferred.reject(xhr);
                }
            });
        
        
        return deferred;
    }
}

export = getDatabaseForStudioCommand;
