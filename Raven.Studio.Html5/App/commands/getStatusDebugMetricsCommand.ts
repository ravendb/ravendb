import commandBase = require("commands/commandBase");
import database = require("models/database");

class getStatusDebugMetricsCommand extends commandBase {

    constructor(private db: database) {
        super();
    }

    execute(): JQueryPromise<statusDebugMetricsDto> {
        var url = "/debug/metrics";
        return this.query<statusDebugMetricsDto>(url, null, this.db);
    }
}

export = getStatusDebugMetricsCommand;