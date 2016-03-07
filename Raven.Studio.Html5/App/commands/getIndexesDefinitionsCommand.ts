import commandBase = require("commands/commandBase");
import database = require("models/database");

class getIndexesDefinitionsCommand extends commandBase {
    constructor(private db: database, private skip = 0, private take = 256) {
        super();
    }

    execute(): JQueryPromise<indexDefinitionListItemDto[]> {
        var args = {
          start: this.skip,
          pageSize: this.take
        };
        var url = "/indexes" + this.urlEncodeArgs(args);
        return this.query(url, null, this.db);
    }
}

export = getIndexesDefinitionsCommand;
