import commandBase = require("commands/commandBase");
import database = require("models/resources/database");

class queryIndexDebugAfterReduceCommand extends commandBase {
    constructor(private indexName: string, private db: database, private reduceKeys:string[]) {
        super();
    }

    execute(): JQueryPromise<any[]> {
        var args = {
            debug: "entries",
            reduceKeys: this.reduceKeys
        };
        var url = "/indexes/" + this.indexName;//TODO: use endpoints
        return this.query(url, args, this.db, r => r.Results);
    }
}

export = queryIndexDebugAfterReduceCommand;
