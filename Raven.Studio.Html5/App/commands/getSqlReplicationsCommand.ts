import commandBase = require("commands/commandBase");
import sqlReplication = require("models/sqlReplication");
import database = require("models/database");

class getSqlReplicationsCommand extends commandBase {

    constructor(private db: database) {
        super();
    }

    execute(): JQueryPromise<Array<sqlReplication>> {
        var args = {
            startsWith: "Raven/SqlReplication/Configuration/",
            exclude: null,
            start: 0,
            pageSize: 256
        };

        return this.query("/docs", args, this.db, (dtos: sqlReplicationDto[]) => dtos.map(dto => new sqlReplication(dto)));
    }
}

export = getSqlReplicationsCommand;