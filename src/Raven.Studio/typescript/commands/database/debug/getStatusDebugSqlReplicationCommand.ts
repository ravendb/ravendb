import commandBase = require("commands/commandBase");
import database = require("models/resources/database");

class getStatusDebugSqlReplicationCommand extends commandBase {

    constructor(private db: database) {
        super();
    }

    execute(): JQueryPromise<sqlReplicationStatsDto[]> {
        var url = "/sql-replication/stats";
        return this.query<sqlReplicationStatsDto[]>(url, null, this.db);
    }
}

export = getStatusDebugSqlReplicationCommand;
