import commandBase = require("commands/commandBase");
import database = require("models/resources/database");

class removeFromClusterUnsafeCommand extends commandBase {


    constructor(private db: database, private connectionInfo: nodeConnectionInfoDto) {
        super();
    }

    execute(): JQueryPromise<any> {
        return this.post("/admin/cluster/remove-unsafe", ko.toJSON(this.connectionInfo), this.db, { dataType: undefined })
            .done(() => this.reportSuccess("Node was forced to be removed from cluster in unsafe way."))
            .fail((response: JQueryXHR) => this.reportError("Failed to remove node from cluster in unsafe way", response.responseText, response.statusText));
    }
}

export = removeFromClusterUnsafeCommand;