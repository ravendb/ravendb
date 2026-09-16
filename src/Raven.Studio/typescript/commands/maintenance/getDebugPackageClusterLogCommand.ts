import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

// per-node raft debug view captured in the package (same shape the live getClusterLogCommand returns)
class getDebugPackageClusterLogCommand extends commandBase {
    constructor(
        private packageId: string,
        private nodeTag: string
    ) {
        super();
    }

    execute(): JQueryPromise<Raven.Server.Rachis.RaftDebugView> {
        const url =
            endpoints.global.debugPackageAnalyzer.debugInfoPackageAnalyzerClusterLog +
            this.urlEncodeArgs({ packageId: this.packageId, nodeTag: this.nodeTag });

        return this.query<Raven.Server.Rachis.RaftDebugView>(url, null, null);
    }
}

export = getDebugPackageClusterLogCommand;
