import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class getDebugPackageDatabaseIndexErrorsCommand extends commandBase {
    constructor(
        private packageId: string,
        private nodeTag: string,
        private databaseName: string
    ) {
        super();
    }

    execute(): JQueryPromise<Raven.Client.Documents.Indexes.IndexErrors[]> {
        const url =
            endpoints.global.debugPackageAnalyzer.debugInfoPackageAnalyzerDatabasesIndexesErrors +
            this.urlEncodeArgs({ packageId: this.packageId, nodeTag: this.nodeTag, name: this.databaseName });

        const extractor = (response: resultsDto<Raven.Client.Documents.Indexes.IndexErrors>) => response.Results;
        return this.query(url, null, null, extractor);
    }
}

export = getDebugPackageDatabaseIndexErrorsCommand;
