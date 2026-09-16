import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

type DebugPackageAnalysisSummary = Raven.Server.Documents.Handlers.Debugging.DebugPackage.DebugPackageAnalysisSummary;

class getDebugPackageAnalysisSummaryCommand extends commandBase {

    constructor(private packageId: string) {
        super();
    }

    execute(): JQueryPromise<DebugPackageAnalysisSummary> {
        const url =
            endpoints.global.debugPackageAnalyzer.debugInfoPackageAnalyzerSummary +
            this.urlEncodeArgs({ packageId: this.packageId });

        return this.query<DebugPackageAnalysisSummary>(url, null, null);
    }
}

export = getDebugPackageAnalysisSummaryCommand;
