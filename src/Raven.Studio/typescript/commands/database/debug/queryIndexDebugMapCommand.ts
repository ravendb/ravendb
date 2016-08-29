import commandBase = require("commands/commandBase");
import database = require("models/resources/database");

class queryIndexDebugMapCommand extends commandBase {
    constructor(private indexName: string, private db: database, private args: queryIndexDebugMapArgsDto, private skip = 0, private take = 256) {
        super();
    }

    execute(): JQueryPromise<any[]> {
        var baseArgs = {
            start: this.skip,
            pageSize: this.take,
            debug: "map"
        };
        
        var url = "/indexes/" + this.indexName;//TODO: use endpoints
        return this.query(url, $.extend({}, baseArgs, this.args), this.db, r => r.Results);
    }
}

export = queryIndexDebugMapCommand;
