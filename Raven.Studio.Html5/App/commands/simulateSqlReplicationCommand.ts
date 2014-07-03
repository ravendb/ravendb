import commandBase = require("commands/commandBase");
import database = require("models/database");


class simulateSqlReplicationCommand extends  commandBase{
    
    constructor(private db: database, private sqlReplicationName: string, private documentId: string) {
        super();
    }

    execute(): JQueryPromise<string[]> {
        var args = {
            documentId: this.documentId,
            sqlReplicationName: this.sqlReplicationName
        };

        return this.query<string[]>("/studio-tasks/simulate-sql-replication",args,this.db);
    }
}

export = simulateSqlReplicationCommand;