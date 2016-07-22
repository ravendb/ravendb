import commandBase = require("commands/commandBase");
import database = require("models/resources/database");

class removeClusteringCommand extends commandBase {


    constructor(private db: database) {
        super();
    }

    execute(): JQueryPromise<any> {
        return this.patch("/admin/cluster/remove-clustering", null, this.db)
            .done(() => this.reportSuccess("Removed cluster entirely and performed cleanup."))
            .fail((response: JQueryXHR) => this.reportError("Unable to remove cluster and perform cleanup", response.responseText, response.statusText));
    }
}

export = removeClusteringCommand;
