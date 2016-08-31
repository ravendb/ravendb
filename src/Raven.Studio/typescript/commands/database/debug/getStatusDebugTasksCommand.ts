import commandBase = require("commands/commandBase");
import database = require("models/resources/database");

class getStatusDebugTasksCommand extends commandBase {

    constructor(private db: database) {
        super();
    }

    execute(): JQueryPromise<taskMetadataDto[]> {
        var url = "/debug/tasks";//TODO: use endpoints
        return this.query<taskMetadataDto[]>(url, null, this.db);
    }
}

export = getStatusDebugTasksCommand;
