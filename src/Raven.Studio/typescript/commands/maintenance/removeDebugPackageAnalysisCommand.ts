import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class removeDebugPackageAnalysisCommand extends commandBase {

    constructor(private packageId: string) {
        super();
    }

    execute(): JQueryPromise<void> {
        const url = endpoints.global.debugPackageAnalyzer.debugInfoPackageAnalyzerRemove +
            this.urlEncodeArgs({ packageId: this.packageId });

        return this.del<void>(url, null, null, { dataType: "text" })
            .fail((response: JQueryXHR) =>
                this.reportError("Failed to remove the debug package analysis", response.responseText, response.statusText));
    }
}

export = removeDebugPackageAnalysisCommand;
