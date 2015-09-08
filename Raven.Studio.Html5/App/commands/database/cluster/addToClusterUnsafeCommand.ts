import commandBase = require("commands/commandBase");
import database = require("models/resources/database");

class addToClusterUnsafeCommand extends commandBase {


    constructor(private db: database, private connectionInfo: nodeConnectionInfoDto) {
        super();
    }

    execute(): JQueryPromise<any> {
        return this.post("/admin/cluster/add-unsafe", ko.toJSON(this.connectionInfo), this.db, { dataType: undefined })
            .done(() => this.reportSuccess("Node was forced to be added to cluster in unsafe way."))
            .fail((response: JQueryXHR) => this.reportError("Failed to add node to cluster in unsafe way", response.responseText, response.statusText));
    }
}

export = addToClusterUnsafeCommand;