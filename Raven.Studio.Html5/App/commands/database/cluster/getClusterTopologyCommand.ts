import commandBase = require("commands/commandBase");
import topology = require("models/database/replication/topology");
import database = require("models/resources/database");

class getClusterTopologyCommand extends commandBase {

    constructor(private ownerDb: database) {
        super();

        if (!this.ownerDb) {
            throw new Error("Must specify a database.");
        }
    }

    execute(): JQueryPromise<topology> {
        return this.query("/cluster/topology", null, this.ownerDb, x => new topology(x));

    }
}

export = getClusterTopologyCommand;