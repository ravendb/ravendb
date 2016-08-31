import commandBase = require("commands/commandBase");
import database = require("models/resources/database");

class stopReducingCommand extends commandBase {

    constructor(private db: database) {
        super();
    }

    execute(): JQueryPromise<any> {
        this.reportInfo("Disabling reduction... (will wait for current reduction batch to complete)");

        var url = '/admin/stopReducing';//TODO: use endpoints
        var createTask = this.post(url, null, this.db);
        createTask.done(() => this.reportSuccess("Reduction was disabled"));
        createTask.fail((response: JQueryXHR) => this.reportError("Failed to disable reduction", response.responseText, response.statusText));

        return createTask;
    }
}

export = stopReducingCommand;
