import commandBase = require("commands/commandBase");
import database = require("models/database");

class getDatabasesCommand extends commandBase {
    
    execute(): JQueryPromise<database[]> {
        var resultsSelector = (databases: databaseDto[]) => databases.map(db => new database(db.Name, db.Disabled));
        return this.query("/databases", { pageSize: 1024 }, null, resultsSelector);
    }
}

export = getDatabasesCommand;