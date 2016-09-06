import commandBase = require("commands/commandBase");
import database = require("models/resources/database");

class getStatusDebugCurrentlyIndexingCommand extends commandBase {

    constructor(private db: database) {
        super();
    }

    execute(): JQueryPromise<statusDebugCurrentlyIndexingDto> {
        var url = "/debug/currently-indexing";//TODO: use endpoints
        return this.query<statusDebugCurrentlyIndexingDto>(url, null, this.db);
    }
}

export = getStatusDebugCurrentlyIndexingCommand;
