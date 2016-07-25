import commandBase = require("commands/commandBase");
import sqlReplication = require("models/database/sqlReplication/sqlReplication");
import database = require("models/resources/database");

class getSqlReplicationsCommand extends commandBase {

    constructor(private db: database, private sqlReplicationName:string = null) {
        super();
    }

    execute(): JQueryPromise<Array<sqlReplication>> {
        var args = {
            startsWith: "Raven/SqlReplication/Configuration/",
            exclude: <string>null,
            start: 0,
            pageSize: 256
        };

        return this.query("/docs", args, this.db, (dtos: sqlReplicationDto[]) => dtos.map(dto => new sqlReplication(dto)));
    }
}

export = getSqlReplicationsCommand;
