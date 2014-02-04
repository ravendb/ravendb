import commandBase = require("commands/commandBase");
import index = require("models/index");
import database = require("models/database");

class deleteIndexCommand extends commandBase {
    constructor(private indexName: string, private db: database) {
        super();
    }

    execute(): JQueryPromise<any> {
        this.reportInfo("Deleting " + this.indexName + "...");
        return this.del("/indexes/" + this.indexName, null, this.db)
            .fail(response => this.reportError("Failed to delete index " + this.indexName, JSON.stringify(response)))
            .done(() => this.reportSuccess("Deleted " + this.indexName));
    }
}

export = deleteIndexCommand;