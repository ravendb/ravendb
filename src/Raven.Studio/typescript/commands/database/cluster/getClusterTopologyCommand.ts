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
        var task = $.Deferred<topology>();
        this.query("/cluster/topology", null, this.ownerDb, x => new topology(x))//TODO: use endpoints
            .done((result: topology) => {
                task.resolve(result);
            })
            .fail((result :JQueryXHR) => {
                if (result.status === 200 && !result.responseText) {
                    task.resolve(null);
                } else {
                    task.reject(result);
                }
            });

        return task;

    }
}

export = getClusterTopologyCommand;
