import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import sqlReplication = require("models/database/sqlReplication/sqlReplication");


class simulateSqlReplicationCommand extends  commandBase{
    
    constructor(private db: database, private simulatedSqlReplication: sqlReplication, private documentId: string, private performRolledbackTransaction) {
        super();
    }

    execute(): JQueryPromise<sqlReplicationSimulationResultDto> {
        var args = {
            documentId: this.documentId,
            performRolledBackTransaction: this.performRolledbackTransaction,
            sqlReplication: JSON.stringify(this.simulatedSqlReplication.toDto())
        };

        return this.post("/studio-tasks/simulate-sql-replication", JSON.stringify(args), this.db, { dataType: undefined }, 60000);
    }
}

export = simulateSqlReplicationCommand;
