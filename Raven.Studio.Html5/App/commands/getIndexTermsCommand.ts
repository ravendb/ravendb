import commandBase = require("commands/commandBase");
import database = require("models/database");

class getIndexTermsCommand extends commandBase {

    constructor(private indexName: string, private field: string, private db: database) {
        super();
    }

    execute(): JQueryPromise<string[]> {
        var urlArgs = {
            field: this.field,
            pageSize: 1024
        };
        var url = "/terms/" + this.indexName + this.urlEncodeArgs(urlArgs);
        return this.query(url, null, this.db);
    }
} 

export = getIndexTermsCommand;