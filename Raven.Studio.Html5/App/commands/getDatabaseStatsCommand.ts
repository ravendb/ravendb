import commandBase = require("commands/commandBase");
import database = require("models/database");

class getDatabaseStatsCommand extends commandBase {

    constructor(private db: database) {
        super();
    }

    execute(): JQueryPromise<databaseStatisticsDto> {
        var url = "/stats";
        return this.query<databaseStatisticsDto>(url, null, this.db);
    }
}

export = getDatabaseStatsCommand;