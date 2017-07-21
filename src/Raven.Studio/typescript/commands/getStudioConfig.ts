import commandBase = require("commands/commandBase");
import database = require("models/resources/database");

class getStudioConfig extends commandBase {

    constructor(private db: database) {
        super();
    }

    execute(): JQueryPromise<void> {
        var url = "/studio-tasks/config";
        var getTask = this.query<void>(url, null, this.db);
        return getTask;
    }
}

export = getStudioConfig;
