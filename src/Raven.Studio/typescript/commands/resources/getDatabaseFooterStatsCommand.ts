import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class getDatabaseFooterStatsCommand extends commandBase {

    constructor(private db: database, private longWait: boolean = false) {
        super();
    }

    execute(): JQueryPromise<Raven.Server.Documents.Studio.FooterStatistics> {
        const url = endpoints.databases.studioStats.studioFooterStats;
        return this.query<Raven.Server.Documents.Studio.FooterStatistics>(url, null, this.db);
    }

    private getQueryUrlFragment(): string {
        return endpoints.databases.stats.stats;
    }
}

export = getDatabaseFooterStatsCommand;
