import commandBase = require("commands/commandBase");
import database = require("models/resources/database");

class getCSharpIndexDefinitionCommand extends commandBase {
    constructor(private indexName: string, private db: database) {
        super();
    }

    execute(): JQueryPromise<string> {
        var url = "/c-sharp-index-definition/" + this.indexName;//TODO: use endpoints
        return this.query(url, null, this.db);
    }
}

export = getCSharpIndexDefinitionCommand;
