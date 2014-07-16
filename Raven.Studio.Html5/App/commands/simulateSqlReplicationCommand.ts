import commandBase = require("commands/commandBase");
import database = require("models/database");
import sqlReplication = require("models/sqlReplication");


class simulateSqlReplicationCommand extends  commandBase{
    
    constructor(private db: database, private simulatedSqlReplication: sqlReplication, private documentId: string) {
        super();
    }

    execute(): JQueryPromise<string[]> {
        var args = {
            documentId: this.documentId,
            sqlReplication: JSON.stringify(this.simulatedSqlReplication.toDto())
        };

        return this.query<string[]>("/studio-tasks/simulate-sql-replication",args,this.db);
    }
}

export = simulateSqlReplicationCommand;