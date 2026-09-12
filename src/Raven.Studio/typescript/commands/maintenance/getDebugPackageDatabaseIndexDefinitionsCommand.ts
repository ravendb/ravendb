import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class getDebugPackageDatabaseIndexDefinitionsCommand extends commandBase {
    constructor(
        private packageId: string,
        private nodeTag: string,
        private databaseName: string
    ) {
        super();
    }

    execute(): JQueryPromise<Raven.Client.Documents.Indexes.IndexDefinition[]> {
        const url =
            endpoints.global.debugPackageAnalyzer.debugInfoPackageAnalyzerDatabasesIndexes +
            this.urlEncodeArgs({ packageId: this.packageId, nodeTag: this.nodeTag, name: this.databaseName });

        const extractor = (response: resultsDto<Raven.Client.Documents.Indexes.IndexDefinition>) => response.Results;
        return this.query(url, null, null, extractor);
    }
}

export = getDebugPackageDatabaseIndexDefinitionsCommand;
