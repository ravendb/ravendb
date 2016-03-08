import commandBase = require("commands/commandBase");
import database = require("models/resources/database");

class getStatusDebugTasksSummaryCommand extends commandBase {

    constructor(private db: database) {
        super();
    }

    execute(): JQueryPromise<taskMetadataSummaryDto[]> {
        var url = "/debug/tasks/summary";
        return this.query<taskMetadataSummaryDto[]>(url, null, this.db);
    }
}

export = getStatusDebugTasksSummaryCommand;
