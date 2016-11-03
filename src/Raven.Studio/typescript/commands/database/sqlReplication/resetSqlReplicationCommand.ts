import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class resetSqlReplicationCommand extends commandBase {
    constructor(private db: database, private sqlReplicationName:string) {
        super();
    }

    execute() {
        const args = {
             name: this.sqlReplicationName
        };
        const url = endpoints.databases.sqlReplication.sqlReplicationReset + this.urlEncodeArgs(args);
        return this.post(url, null, this.db, { dataType: undefined })
            .fail(() => this.reportError("SQL replication '" + this.sqlReplicationName + "' failed to reset!"));
    }
}
export = resetSqlReplicationCommand;
