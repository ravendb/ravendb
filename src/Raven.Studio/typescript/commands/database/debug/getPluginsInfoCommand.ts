import commandBase = require("commands/commandBase");
import database = require("models/resources/database");

class getLogsCommand extends commandBase {

    constructor(private db: database) {
        super();
    }

    execute(): JQueryPromise<pluginsInfoDto> {
        var url = "/debug/plugins";//TODO: use endpoints
        return this.query<pluginsInfoDto>(url, null, this.db);
    }
}

export = getLogsCommand;
