import commandBase = require("commands/commandBase");
import database = require("models/resources/database");

class leaveRaftClusterCommand extends commandBase {


    constructor(private db: database, private connectionInfo: nodeConnectionInfoDto) {
        super();
    }

    execute(): JQueryPromise<any> {
        return this.query("/admin/cluster/leave", { name: this.connectionInfo.Name }, this.db)//TODO: use endpoints
            .done(() => this.reportSuccess("Node was removed from cluster."))
            .fail((response: JQueryXHR) => this.reportError("Failed to remove node from cluster", response.responseText, response.statusText));
    }
}

export = leaveRaftClusterCommand;
