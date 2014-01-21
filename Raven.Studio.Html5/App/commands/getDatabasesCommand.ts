import commandBase = require("commands/commandBase");
import database = require("models/database");

class getDatabasesCommand extends commandBase {
    
    execute(): JQueryPromise<database[]> {
        var resultsSelector = (databaseNames: any) => {
            if (databaseNames === "[]") { // Raven 3 returns this when there are no databases. Bug? It should really return an empty array literal. 
                return [];
            }
            return databaseNames.map(n => new database(n));
        }
        return this.query("/databases", { pageSize: 1024 }, null, resultsSelector);
    }
}

export = getDatabasesCommand;