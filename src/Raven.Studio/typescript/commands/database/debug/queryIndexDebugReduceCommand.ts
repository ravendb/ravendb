import commandBase = require("commands/commandBase");
import database = require("models/resources/database");

class queryIndexDebugReduceCommand extends commandBase {
    constructor(private indexName: string, private db: database, private level: number, private key?: string, private skip = 0, private take = 256) {
        super();
    }

    execute(): JQueryPromise<mappedResultInfo[]> {
        var args = {
            start: this.skip,
            pageSize: this.take,
            debug: "reduce",
            key: this.key,
            level: this.level
        };
        var url = "/indexes/" + this.indexName;//TODO: use endpoints
        return this.query(url, args, this.db, r => r.Results);
    }
}

export = queryIndexDebugReduceCommand;
