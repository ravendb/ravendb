import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

// per-node storage I/O analysis of the package (device state, per database I/O, shared index journals)
class getDebugPackageStorageIoCommand extends commandBase {
    constructor(
        private packageId: string,
        private nodeTag: string
    ) {
        super();
    }

    execute(): JQueryPromise<Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Results.StorageIoAnalysisInfo> {
        const url =
            endpoints.global.debugPackageAnalyzer.debugInfoPackageAnalyzerStorageIo +
            this.urlEncodeArgs({ packageId: this.packageId, nodeTag: this.nodeTag });

        return this.query<Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Results.StorageIoAnalysisInfo>(
            url,
            null,
            null
        );
    }
}

export = getDebugPackageStorageIoCommand;
