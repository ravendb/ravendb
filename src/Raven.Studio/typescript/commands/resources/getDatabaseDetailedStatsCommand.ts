import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class getDatabaseDetailedStatsCommand extends commandBase {

    constructor(private db: database, private longWait: boolean = false) {
        super();
    }

    execute(): JQueryPromise<Raven.Client.Documents.Operations.DetailedDatabaseStatistics> {
        
        const url = endpoints.databases.stats.statsDetailed;
        
        return this.query<Raven.Client.Documents.Operations.DetailedDatabaseStatistics>(url, null, this.db, null, null, this.getTimeToAlert(this.longWait))
            .fail((response: JQueryXHR) => this.reportError("Failed to get the database stats details", response.responseText, response.statusText));
    }
}

export = getDatabaseDetailedStatsCommand;
