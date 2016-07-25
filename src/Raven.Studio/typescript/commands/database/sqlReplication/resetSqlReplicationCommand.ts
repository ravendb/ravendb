import commandBase = require("commands/commandBase");
import database = require("models/resources/database");

class resetSqlReplicationCommand extends commandBase {
    constructor(private db: database, private sqlReplicationName:string) {
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
