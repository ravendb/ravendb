import commandBase = require("commands/commandBase");
import database = require("models/resources/database");

class getIndexDefinitionCommand extends commandBase {
    constructor(private indexName: string, private db: database) {
        super();
    }

    execute(): JQueryPromise<indexDefinitionContainerDto> {
        var url = "/indexes/" + this.indexName + "?definition=yes";
        return this.query(url, null, this.db);
    }
}

export = getIndexDefinitionCommand;