import commandBase = require("commands/commandBase");
import database = require("models/resources/database");

class getSqlReplicationPerfStatsCommand extends commandBase {

    constructor(private db: database) {
        super();
    }

    execute(): JQueryPromise<any> {
        var url = "/debug/sql-replication-perf-stats";
        return this.query<any>(url, null, this.db);
    }
}

export = getSqlReplicationPerfStatsCommand;