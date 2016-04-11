import commandBase = require("commands/commandBase");

class getSqlReplicationStatsCommand extends commandBase {
    constructor(private ownerDb, private sqlReplicationName) {
        super();

        if (!this.ownerDb) {
            throw new Error("Must specify a database.");
        }
    }

    execute(){
        var args = {
            sqlReplicationName: this.sqlReplicationName
        };

        var url = "/sql-replication/stats";

        var resultsSelector = function (result) {
            result.Value["Name"] = result.Key;
            var replicationDto = result.Value;
            return replicationDto;
        };
        return this.query(url, args, this.ownerDb, resultsSelector);
    }
}

export = getSqlReplicationStatsCommand;
