import commandBase = require("commands/commandBase");

class resetSqlReplicationCommand extends commandBase {
    constructor(private db, private sqlReplicationName) {
        super();
    }

    execute() {
        var args = { sqlReplicationName: this.sqlReplicationName };
        var url = "/studio-tasks/reset-sql-replication" + super.urlEncodeArgs(args);
        return this.post(url, null, this.db)
            .fail(() => this.reportError("SQL replication '" + this.sqlReplicationName + "' failed to reset!"));
    }
}
export = resetSqlReplicationCommand;
