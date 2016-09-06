import commandBase = require("commands/commandBase");
import database = require("models/resources/database");

class getRunningTasksCommand extends commandBase {

    constructor(private db: database) {
        super();
    }

    execute(): JQueryPromise<runningTaskDto[]> {
        var url = "/operations";//TODO: use endpoints
        return this.query<runningTaskDto[]>(url, null, this.db);
    }
}

export = getRunningTasksCommand;
