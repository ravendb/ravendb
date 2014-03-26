import commandBase = require("commands/commandBase");
import replicationSource = require("models/replicationSource");
import database = require("models/database");

class getReplicationSourcesCommand extends commandBase {
    constructor(private db: database) {
        super();
    }

    execute(): JQueryPromise<Array<replicationSource>> {
        var args = {
            startsWith: "Raven/Replication/Sources",
            exclude: null,
            start: 0,
            pageSize: 1024
        };

        return this.query("/docs", args, this.db, (dtos: replicationSourceDto[]) => {
            var sources = dtos.map(dto => new replicationSource(dto));
            // and push current db as replication source
            //TODO: this.db.name returns database name instead of entire url!
            sources.push(new replicationSource({ ServerInstanceId: this.db.statistics().DatabaseId, Source: this.db.name }));
            return sources;

        });
    }
}

export = getReplicationSourcesCommand;