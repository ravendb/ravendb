import commandBase = require("commands/commandBase");
import database = require("models/resources/database");

class getIndexesDefinitionsCommand extends commandBase {
    constructor(private db: database) {
        super();
    }

    execute(): JQueryPromise<indexDefinitionListItemDto[]> {
        var url = "/indexes";
        return this.query(url, null, this.db);
    }
}

export = getIndexesDefinitionsCommand;