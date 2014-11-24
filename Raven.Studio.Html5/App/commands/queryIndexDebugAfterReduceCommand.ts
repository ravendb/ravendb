import commandBase = require("commands/commandBase");
import database = require("models/database");
import document = require("models/document");

class queryIndexDebugAfterReduceCommand extends commandBase {
    constructor(private indexName: string, private db: database, private reduceKeys:string[]) {
        super();
    }

    execute(): JQueryPromise<any[]> {
        var args = {
            debug: "entries",
            reduceKeys: this.reduceKeys.join(",")
        };
        var url = "/indexes/" + this.indexName;
        return this.query(url, args, this.db, r => r.Results);
    }
}

export = queryIndexDebugAfterReduceCommand;