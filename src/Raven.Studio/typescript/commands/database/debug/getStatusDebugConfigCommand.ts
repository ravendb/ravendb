import commandBase = require("commands/commandBase");
import database = require("models/resources/database");

class getStatusDebugConfigCommand extends commandBase {

    constructor(private db: database) {
        super();
    }

    execute(): JQueryPromise<any> {
        var url = "/debug/config";//TODO: use endpoints
        return this.query<any>(url, null, this.db);
    }
}

export = getStatusDebugConfigCommand;
