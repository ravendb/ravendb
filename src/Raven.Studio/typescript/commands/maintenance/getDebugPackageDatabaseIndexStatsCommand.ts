import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class getDebugPackageDatabaseIndexStatsCommand extends commandBase {
    constructor(
        private packageId: string,
        private nodeTag: string,
        private databaseName: string
    ) {
        super();
    }

    execute(): JQueryPromise<Raven.Client.Documents.Indexes.IndexStats[]> {
        const url =
            endpoints.global.debugPackageAnalyzer.debugInfoPackageAnalyzerDatabasesIndexesStats +
            this.urlEncodeArgs({ packageId: this.packageId, nodeTag: this.nodeTag, name: this.databaseName });

        const extractor = (response: resultsDto<Raven.Client.Documents.Indexes.IndexStats>) => response.Results;
        return this.query(url, null, null, extractor);
    }
}

export = getDebugPackageDatabaseIndexStatsCommand;
