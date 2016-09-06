import commandBase = require("commands/commandBase");
import database = require("models/resources/database");

class getClusterNodesStatusCommand extends commandBase {

    constructor(private db: database) {
        super();
    }

    execute(): JQueryPromise<any> {
        return this.query("/cluster/status", null, this.db);//TODO: use endpoints
    }
}

export = getClusterNodesStatusCommand;
