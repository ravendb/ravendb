import commandBase = require("commands/commandBase");
import database = require("models/database");

class getDatabasesCommand extends commandBase {
    
    execute(): JQueryPromise<database[]> {
        var resultsSelector = (databaseNames: any) => databaseNames.map(n => new database(n));
        return this.query("/databases", { pageSize: 1024 }, null, resultsSelector);
    }
}

export = getDatabasesCommand;