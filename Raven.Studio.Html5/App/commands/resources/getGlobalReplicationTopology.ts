import commandBase = require("commands/commandBase");
import appUrl = require("common/appUrl");

class getGlobalReplicationTopology extends commandBase {

    constructor(private databases: boolean, private filesystems: boolean, private counters: boolean) {
        super();
    } 

    execute(): JQueryPromise<globalTopologyDto> {

        var args = {
            Databases: this.databases,
            Filesystems: this.filesystems,
            Counters: this.counters
        };
        
        return this.post("/admin/replication/topology/global", ko.toJSON(args), appUrl.getSystemDatabase(), null, 20000).then((result) => {
            return result;
        });
    }
}

export = getGlobalReplicationTopology;
