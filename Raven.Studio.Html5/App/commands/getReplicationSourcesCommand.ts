import commandBase = require("commands/commandBase");
import database = require("models/database");

class getReplicationSourcesCommand extends commandBase {
    constructor(private db: database) {
        super();
    }

    execute(): JQueryPromise<dictionary<string>> {
        var args = {
            startsWith: "Raven/Replication/Sources",
            exclude: null,
            start: 0,
            pageSize: 1024
        };

        return this.query("/docs", args, this.db, (dtos: replicationSourceDto[]) => {
            var result: dictionary<string> = {};
            // insert remote databases info
            dtos.forEach(v => result[v.ServerInstanceId] = database.getNameFromUrl(v.Source));
            // ... and insert local database instance id
            result[this.db.statistics().DatabaseId] = this.db.name;

            return result;
        });
    }
}

export = getReplicationSourcesCommand;