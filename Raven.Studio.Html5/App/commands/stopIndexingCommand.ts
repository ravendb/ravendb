import commandBase = require("commands/commandBase");
import database = require("models/database");

class stopIndexingCommand extends commandBase {

    constructor(private db: database) {
        super();
    }

    execute(): JQueryPromise<any> {
        this.reportInfo("Disabling Indexing... (will wait for current indexing batch to complete)");

        var url = '/admin/stopIndexing';
        var createTask = this.post(url, null, this.db);
        createTask.done(() => this.reportSuccess("Indexing was disabled"));
        createTask.fail((response: JQueryXHR) => this.reportError("Failed to disable indexing", response.responseText, response.statusText));

        return createTask;
    }
}

export = stopIndexingCommand;