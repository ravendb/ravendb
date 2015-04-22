import commandBase = require("commands/commandBase");
import database = require("models/resources/database");

class getReplicationTopology extends commandBase {

    constructor(private db: database) {
        super();
    }

    execute(): JQueryPromise<replicationTopologyDto> {
        return this.post("/admin/replication/topology/view", null, this.db).then((result) => {
            return result;
        });
    }
}

export = getReplicationTopology;