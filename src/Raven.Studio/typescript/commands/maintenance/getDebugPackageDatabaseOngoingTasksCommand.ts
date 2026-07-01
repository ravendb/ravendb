import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

// per-node ongoing tasks captured in the package (same OngoingTasksResult shape the live getOngoingTasksCommand returns)
class getDebugPackageDatabaseOngoingTasksCommand extends commandBase {
    constructor(
        private packageId: string,
        private nodeTag: string,
        private databaseName: string
    ) {
        super();
    }

    execute(): JQueryPromise<Raven.Server.Web.System.OngoingTasksResult> {
        const url =
            endpoints.global.debugPackageAnalyzer.debugInfoPackageAnalyzerDatabasesTasks +
            this.urlEncodeArgs({ packageId: this.packageId, nodeTag: this.nodeTag, name: this.databaseName });

        return this.query<Raven.Server.Web.System.OngoingTasksResult>(url, null, null);
    }
}

export = getDebugPackageDatabaseOngoingTasksCommand;
