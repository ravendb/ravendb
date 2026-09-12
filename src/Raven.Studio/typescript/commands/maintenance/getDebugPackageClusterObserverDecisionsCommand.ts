import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

// per-node cluster observer decisions captured in the package (same shape the live getClusterObserverDecisionsCommand returns)
class getDebugPackageClusterObserverDecisionsCommand extends commandBase {
    constructor(
        private packageId: string,
        private nodeTag: string
    ) {
        super();
    }

    execute(): JQueryPromise<Raven.Server.ServerWide.Maintenance.ClusterObserverDecisions> {
        const url =
            endpoints.global.debugPackageAnalyzer.debugInfoPackageAnalyzerClusterObserverDecisions +
            this.urlEncodeArgs({ packageId: this.packageId, nodeTag: this.nodeTag });

        return this.query<Raven.Server.ServerWide.Maintenance.ClusterObserverDecisions>(url, null, null);
    }
}

export = getDebugPackageClusterObserverDecisionsCommand;
