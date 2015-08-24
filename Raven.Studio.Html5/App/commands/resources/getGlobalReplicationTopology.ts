import commandBase = require("commands/commandBase");
import appUrl = require("common/appUrl");

class getGlobalReplicationTopology extends commandBase {

    execute(): JQueryPromise<globalTopologyDto> {
        return this.post("/admin/replication/topology/global", null, appUrl.getSystemDatabase()).then((result) => {
            return result;
        });
    }
}

export = getGlobalReplicationTopology;