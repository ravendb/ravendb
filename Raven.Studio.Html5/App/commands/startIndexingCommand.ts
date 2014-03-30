import commandBase = require("commands/commandBase");
import database = require("models/database");

class startIndexingCommand extends commandBase {

    constructor(private db: database) {
        super();
    }

    execute(): JQueryPromise<any> {
      this.reportInfo("Enabling indexing...");

        var url = '/admin/startIndexing';
        var createTask = this.post(url, null, this.db);
        createTask.done(() => this.reportSuccess("Indexing was enabled"));
        createTask.fail((response: JQueryXHR) => this.reportError("Failed to enable indexing", response.responseText, response.statusText));

        return createTask;
    }
}

export = startIndexingCommand;