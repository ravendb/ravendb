import commandBase = require("commands/commandBase");
import database = require("models/database");

class getReplicationStatsCommand extends commandBase {
    constructor(private db: database) {
        super();
    }

    execute(): JQueryPromise<replicationStatsDocumentDto> {
        return this.query("/replication/info", null, this.db);
    }
}

export = getReplicationStatsCommand;