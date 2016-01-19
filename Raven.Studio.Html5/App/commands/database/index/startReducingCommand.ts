import commandBase = require("commands/commandBase");
import database = require("models/resources/database");

class startReducingCommand extends commandBase {

    constructor(private db: database) {
        super();
    }

    execute(): JQueryPromise<any> {
      this.reportInfo("Enabling reduction...");

        var url = '/admin/startReducing';
        var createTask = this.post(url, null, this.db);
        createTask.done(() => this.reportSuccess("Reduction was enabled"));
        createTask.fail((response: JQueryXHR) => this.reportError("Failed to enable reduction", response.responseText, response.statusText));

        return createTask;
    }
}

export = startReducingCommand;
