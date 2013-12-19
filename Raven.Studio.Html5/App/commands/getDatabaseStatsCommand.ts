import commandBase = require("commands/commandBase");
import database = require("models/database");

class getDatabaseStatsCommand extends commandBase {

    constructor(private db: database) {
        super();
    }

    execute(): JQueryPromise<databaseStatisticsDto> {
        var url = this.db.isSystem ? "/stats" : "/databases/" + this.db.name + "/stats";
        return this.query<databaseStatisticsDto>(url, null, null);
    }
}

export = getDatabaseStatsCommand;