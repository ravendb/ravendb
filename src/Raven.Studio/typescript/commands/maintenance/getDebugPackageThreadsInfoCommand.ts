import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

type ThreadsInfo = Raven.Server.Dashboard.ThreadsInfo;

class getDebugPackageThreadsInfoCommand extends commandBase {
    constructor(
        private packageId: string,
        private nodeTag: string
    ) {
        super();
    }

    execute(): JQueryPromise<ThreadsInfo> {
        const url =
            endpoints.global.debugPackageAnalyzer.debugInfoPackageAnalyzerThreadsRunaway +
            this.urlEncodeArgs({ packageId: this.packageId, nodeTag: this.nodeTag });

        return this.query<ThreadsInfo>(url, null, null);
    }
}

export = getDebugPackageThreadsInfoCommand;
