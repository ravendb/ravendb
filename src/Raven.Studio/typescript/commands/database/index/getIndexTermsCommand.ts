import commandBase = require("commands/commandBase");
import database = require("models/resources/database");

class getIndexTermsCommand extends commandBase {

    constructor(private indexName: string, private field: string, private db: database) {
        super();
    }

    execute(): JQueryPromise<string[]> {
        var urlArgs = {
            field: this.field,
            name: this.indexName, 
            pageSize: 1024
        };
        var url = "/indexes/terms" + this.urlEncodeArgs(urlArgs);//TODO: use endpoints
        return this.query(url, null, this.db);
    }
} 

export = getIndexTermsCommand;
