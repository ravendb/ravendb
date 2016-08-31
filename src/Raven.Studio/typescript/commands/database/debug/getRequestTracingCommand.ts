import commandBase = require("commands/commandBase");
import database = require("models/resources/database");

class getRequestTracingCommand extends commandBase {

    constructor(private db: database) {
        super();
    }

    execute(): JQueryPromise<requestTracingDto[]> {
        var url = "/debug/request-tracing";//TODO: use endpoints
        return this.query<requestTracingDto[]>(url, null, this.db);
    }
}

export = getRequestTracingCommand;
