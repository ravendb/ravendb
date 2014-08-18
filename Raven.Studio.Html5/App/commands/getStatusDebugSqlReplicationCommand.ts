import commandBase = require("commands/commandBase");
import database = require("models/database");

class getStatusDebugSqlReplicationCommand extends commandBase {

    constructor(private db: database) {
        super();
    }

    execute(): JQueryPromise<sqlReplicationStatisticsDto[]> {
        var url = "/debug/sql-replication-stats";
        return this.query<sqlReplicationStatisticsDto[]>(url, null, this.db);
    }
}

export = getStatusDebugSqlReplicationCommand;