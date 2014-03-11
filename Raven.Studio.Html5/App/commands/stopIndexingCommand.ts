import commandBase = require("commands/commandBase");
import database = require("models/database");

class stopIndexingCommand extends commandBase {

    constructor(private db: database) {
        super();
    }

    execute(): JQueryPromise<any> {
        this.reportInfo("Stop indexing...");

        var url = '/admin/stopIndexing';
        var createTask = this.put(url, null, this.db, { dataType: undefined });
        createTask.done(() => this.reportSuccess("Indexing was stopped"));
        createTask.fail((response) => this.reportError("Failed to stop indexing", JSON.stringify(response)));

        return createTask;
    }
}

export = stopIndexingCommand;