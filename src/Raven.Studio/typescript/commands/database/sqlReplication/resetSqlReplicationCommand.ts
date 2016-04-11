import commandBase = require("commands/commandBase");

class resetSqlReplicationCommand extends commandBase {
    constructor(private db, private sqlReplicationName) {
        super();
    }

    execute() {
        var args = { sqlReplicationName: this.sqlReplicationName };
        var url = "/sql-replication/reset" + super.urlEncodeArgs(args);
        return this.post(url, null, this.db);
    }
}
export = resetSqlReplicationCommand;
