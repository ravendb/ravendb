import commandBase = require("commands/commandBase");
import database = require("models/database");
import document = require("models/document");

class queryIndexDebugMapCommand extends commandBase {
    constructor(private indexName: string, private db: database, private key?: string, private skip = 0, private take = 256) {
        super();
    }

    execute(): JQueryPromise<mappedResultInfo[]> {
        var args = {
            start: this.skip,
            pageSize: this.take,
            debug: "map",
            key: this.key
        };
        var url = "/indexes/" + this.indexName;
        return this.query(url, args, this.db, r => r.Results);
    }
}

export = queryIndexDebugMapCommand;