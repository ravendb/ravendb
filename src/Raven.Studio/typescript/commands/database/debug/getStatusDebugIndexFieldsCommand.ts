import commandBase = require("commands/commandBase");
import database = require("models/resources/database");

class getStatusDebugIndexFieldsCommand extends commandBase {

    constructor(private db: database, private indexStr: string) {
        super();
    }

    execute(): JQueryPromise<statusDebugIndexFieldsDto> {
        var url = "/debug/index-fields";//TODO: use endpoints
        return this.post(url, this.indexStr, this.db);
    }
}

export = getStatusDebugIndexFieldsCommand;
