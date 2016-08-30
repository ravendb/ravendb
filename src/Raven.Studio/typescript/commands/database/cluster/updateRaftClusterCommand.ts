import commandBase = require("commands/commandBase");
import database = require("models/resources/database");

class updateRaftClusterCommand extends commandBase {


    constructor(private db: database, private connectionInfo: nodeConnectionInfoDto) {
        super();
    }

    execute(): JQueryPromise<any> {
        return this.post("/admin/cluster/update", ko.toJSON(this.connectionInfo), this.db, { dataType: undefined })//TODO: use endpoints
            .done(() => this.reportSuccess("Configuration was updated"))
            .fail((response: JQueryXHR) => this.reportError("Failed to update node configuration", response.responseText, response.statusText));
    }
}

export = updateRaftClusterCommand;
