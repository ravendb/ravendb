import commandBase = require("commands/commandBase");
import database = require("models/database");

class getStatusDebugSqlReplicationCommand extends commandBase {

    constructor(private db: database) {
        super();
    }

    execute(): JQueryPromise<sqlReplicationStatisticsDto[]> {
        var url = "/debug/sql-replication-stats";
        var resultsSelector = (result) => $.map(result, (value, key) => {
            value["Name"] = key;
            return value;
        });
        return this.query<sqlReplicationStatisticsDto[]>(url, null, this.db, resultsSelector);
    }
}

export = getStatusDebugSqlReplicationCommand;