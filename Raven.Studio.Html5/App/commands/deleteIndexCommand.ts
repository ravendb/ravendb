import commandBase = require("commands/commandBase");
import index = require("models/index");
import database = require("models/database");

class deleteIndexCommand extends commandBase {
    constructor(private index: index, private db: database) {
        super();
    }

    execute(): JQueryPromise<any> {
        this.reportInfo("Deleting " + this.index.name + "...");
        return this.del("/indexes/" + this.index.name, null, this.db)
            .fail(response => this.reportError("Failed to delete index " + this.index.name, JSON.stringify(response)))
            .done(() => this.reportSuccess("Deleted " + this.index.name));
    }
}

export = deleteIndexCommand;