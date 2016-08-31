import commandBase = require("commands/commandBase");
import database = require("models/resources/database");

class killRunningTaskCommand extends commandBase {

    constructor(private db: database, private taskId: number) {
        super();
    }

    execute(): JQueryPromise<any> {
        var url = "/operation/kill";//TODO: use endpoints
        var args = {
            id: this.taskId
        }
        return this.query<runningTaskDto[]>(url, args, this.db);
    }
}

export = killRunningTaskCommand;
