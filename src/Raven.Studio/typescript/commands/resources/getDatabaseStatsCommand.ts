import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import appUrl = require("common/appUrl");
import endpoints = require("endpoints");

class getDatabaseStatsCommand extends commandBase {

    constructor(private db: database, private longWait: boolean = false) {
        super();
    }

    execute(): JQueryPromise<Raven.Client.Documents.Operations.DatabaseStatistics> {
        const url = this.getQueryUrlFragment();
        return this.query<Raven.Client.Documents.Operations.DatabaseStatistics>(url, null, this.db, null, null, this.getTimeToAlert(this.longWait))
            .fail((response: JQueryXHR) => this.reportError("Failed to get database statistics", response.responseText, response.statusText));
    }

    getQueryUrl(): string {
        return appUrl.forDatabaseQuery(this.db) + this.getQueryUrlFragment();
    }

    private getQueryUrlFragment(): string {
        return endpoints.databases.stats.stats;
    }
}

export = getDatabaseStatsCommand;
