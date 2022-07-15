import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class getDatabaseDetailedStatsCommand extends commandBase {

    constructor(private db: database, private location: databaseLocationSpecifier, private longWait: boolean = false) {
        super();
    }

    execute(): JQueryPromise<Raven.Client.Documents.Operations.DetailedDatabaseStatistics> {
        
        const url = endpoints.databases.stats.statsDetailed;
        const args = {
            ...this.location
        };
        
        return this.query<Raven.Client.Documents.Operations.DetailedDatabaseStatistics>(url, args, this.db, null, null, this.getTimeToAlert(this.longWait));
    }
}

export = getDatabaseDetailedStatsCommand;
