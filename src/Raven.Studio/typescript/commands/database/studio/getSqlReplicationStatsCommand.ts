import commandBase = require("commands/commandBase");
import database = require("models/resources/database");

class getSqlReplicationStatsCommand extends commandBase {
    constructor(private ownerDb: database, private sqlReplicationName: string) {
        super();

        if (!this.ownerDb) {
            throw new Error("Must specify a database.");
        }
    }

    execute() {
        var args = {
            sqlReplicationName: this.sqlReplicationName
        };

        var url = "/studio-tasks/get-sql-replication-stats";//TODO: use endpoints

        var resultsSelector = function (result: any) {
            result.Value["Name"] = result.Key;
            var replicationDto = result.Value;
            return replicationDto;
        };
        return this.query(url, args, this.ownerDb, resultsSelector);
    }
}

export = getSqlReplicationStatsCommand;
