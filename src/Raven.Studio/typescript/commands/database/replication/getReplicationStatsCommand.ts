import commandBase = require("commands/commandBase");
import database = require("models/resources/database");

class getReplicationStatsCommand extends commandBase {
    constructor(private db: database) {
        super();
    }

    execute(): JQueryPromise<replicationStatsDocumentDto> {
        return this.query("/replication/info", null, this.db);//TODO: use endpoints
    }
}

export = getReplicationStatsCommand;
