import commandBase = require("commands/commandBase");
import database = require("models/resources/database");

class getIndexMergeSuggestionsCommand extends commandBase {

    constructor(private db: database) {
        super();
    }

    execute(): JQueryPromise<string[]> {
        var url = "/debug/suggest-index-merge";
        return this.query(url, null, this.db);
    }
} 

export = getIndexMergeSuggestionsCommand;