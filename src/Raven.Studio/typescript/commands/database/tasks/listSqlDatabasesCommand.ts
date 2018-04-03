import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class listSqlDatabasesCommand extends commandBase {

    constructor(private db: database, private sourceDatabase: Raven.Server.SqlMigration.Model.SourceSqlDatabase) {
        super();
    }

    execute(): JQueryPromise<Array<string>> {
        const url = endpoints.databases.sqlMigration.adminSqlMigrationListDatabaseNames;
        
        const task = $.Deferred<Array<string>>();
        
        this.post(url, JSON.stringify(this.sourceDatabase), this.db)
            .done(x => {
                task.resolve(x.Result);
            })
            .fail((response: JQueryXHR) => { 
                this.reportError("Failed to list database names", response.responseText, response.statusText);
                task.reject(response);
            });
        
        return task;
    }
}

export = listSqlDatabasesCommand; 
