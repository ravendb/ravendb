import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

// the threads/stack-trace dump groups threads by identical stack (merged stack traces); the shape
// matches the live captureStackTraces import format (rawStackTraceResponseItem)
class getDebugPackageThreadsStackTraceCommand extends commandBase {
    constructor(
        private packageId: string,
        private nodeTag: string
    ) {
        super();
    }

    execute(): JQueryPromise<rawStackTraceResponseItem[]> {
        const url =
            endpoints.global.debugPackageAnalyzer.debugInfoPackageAnalyzerThreadsStackTrace +
            this.urlEncodeArgs({ packageId: this.packageId, nodeTag: this.nodeTag });

        const extractor = (response: { Results: rawStackTraceResponseItem[] }) => response.Results;
        return this.query(url, null, null, extractor);
    }
}

export = getDebugPackageThreadsStackTraceCommand;
