import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

type NetworkAnalysisInfo = Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Results.NetworkAnalysisInfo;

class getDebugPackageNetworkInfoCommand extends commandBase {
    constructor(
        private packageId: string,
        private nodeTag: string
    ) {
        super();
    }

    execute(): JQueryPromise<NetworkAnalysisInfo> {
        const url =
            endpoints.global.debugPackageAnalyzer.debugInfoPackageAnalyzerNetwork +
            this.urlEncodeArgs({ packageId: this.packageId, nodeTag: this.nodeTag });

        return this.query<NetworkAnalysisInfo>(url, null, null);
    }
}

export = getDebugPackageNetworkInfoCommand;
