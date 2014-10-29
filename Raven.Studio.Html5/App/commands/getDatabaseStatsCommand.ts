import commandBase = require("commands/commandBase");
import database = require("models/database");
import appUrl = require("common/appUrl");

class getDatabaseStatsCommand extends commandBase {

    constructor(private db: database) {
        super();
    }

    execute(): JQueryPromise<databaseStatisticsDto> {
        var url = this.getQueryUrlFragment();
        return this.query<databaseStatisticsDto>(url, null, this.db);
    }

    getQueryUrl(): string {
        return appUrl.forResourceQuery(this.db) + this.getQueryUrlFragment();
    }

    private getQueryUrlFragment(): string {
        return "/stats";
    }
}

export = getDatabaseStatsCommand;