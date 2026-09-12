import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class getDebugPackageDatabaseIndexPerformanceCommand extends commandBase {
    constructor(
        private packageId: string,
        private nodeTag: string,
        private databaseName: string
    ) {
        super();
    }

    execute(): JQueryPromise<Raven.Client.Documents.Indexes.IndexPerformanceStats[]> {
        const url =
            endpoints.global.debugPackageAnalyzer.debugInfoPackageAnalyzerDatabasesIndexesPerformance +
            this.urlEncodeArgs({ packageId: this.packageId, nodeTag: this.nodeTag, name: this.databaseName });

        const extractor = (response: { Results: Raven.Client.Documents.Indexes.IndexPerformanceStats[] }) =>
            response.Results;
        return this.query(url, null, null, extractor);
    }
}

export = getDebugPackageDatabaseIndexPerformanceCommand;
