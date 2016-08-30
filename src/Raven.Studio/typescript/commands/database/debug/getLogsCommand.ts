import commandBase = require("commands/commandBase");
import database = require("models/resources/database");

class getLogsCommand extends commandBase {

    constructor(private db: database) {
        super();
    }

    execute(): JQueryPromise<logDto[]> {
        var url = "/logs";//TODO: use endpoints
        return this.query<logDto[]>(url, null, this.db);
    }
}

export = getLogsCommand;
