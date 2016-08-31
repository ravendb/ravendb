import commandBase = require("commands/commandBase");
import database = require("models/resources/database");

class queryIndexDebugDocsCommand extends commandBase {
    constructor(private indexName: string, private db: database, private startsWith: string, private skip = 0, private take = 256) {
        super();
    }

    execute(): JQueryPromise<any[]> {
        var args = {
            start: this.skip,
            pageSize: this.take,
            debug: "docs",
            startsWith: this.startsWith
        };
        
        var url = "/indexes/" + this.indexName;//TODO: use endpoints
        return this.query(url, args, this.db, r => r.Results);
    }
}

export = queryIndexDebugDocsCommand;
