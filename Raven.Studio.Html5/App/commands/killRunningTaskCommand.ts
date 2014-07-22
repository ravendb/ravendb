import commandBase = require("commands/commandBase");
import database = require("models/database");

class getRunningTasksCommand extends commandBase {

    constructor(private db: database, private task: runningTaskDto) {
        super();
    }

    execute(): JQueryPromise<any> {
        var url = "/operation/kill";
        var args = {
            id: this.task.Id
        }
        return this.query<runningTaskDto[]>(url, args, this.db);
    }
}

export = getRunningTasksCommand;