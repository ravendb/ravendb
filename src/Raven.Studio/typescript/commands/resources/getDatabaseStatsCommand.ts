import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import appUrl = require("common/appUrl");

class getDatabaseStatsCommand extends commandBase {

    constructor(private db: database, private longWait: boolean = false) {
        super();
    }

    execute(): JQueryPromise<databaseStatisticsDto> {
        var url = this.getQueryUrlFragment();
        return this.query<databaseStatisticsDto>(url, null, this.db, null, this.getTimeToAlert(this.longWait));
    }

    getQueryUrl(): string {
        return appUrl.forResourceQuery(this.db) + this.getQueryUrlFragment();
    }

    private getQueryUrlFragment(): string {
        return "/stats";//TODO: use endpoints
    }
}

export = getDatabaseStatsCommand;
