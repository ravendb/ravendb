import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class getIndexesProgressCommand extends commandBase {

    constructor(private readonly db: database | string, private readonly location: databaseLocationSpecifier, private readonly indexNames: string[] = null, private readonly exact = false, private readonly excludedIndexNames: string[] = null) {
        super();
    }

    execute(): JQueryPromise<Raven.Client.Documents.Indexes.IndexProgress[]> {
        const args = {
            ...this.location,
            name: this.indexNames?.length ? this.indexNames : undefined,
            exact: this.exact ? true : undefined,
            excludeName: this.excludedIndexNames?.length ? this.excludedIndexNames : undefined,
        };
        const url = endpoints.databases.index.indexesProgress + this.urlEncodeArgs(args);
        const extractor = (response: resultsDto<Raven.Client.Documents.Indexes.IndexProgress>) => response.Results;
        return this.query(url, null, this.db, extractor);
    }
}

export = getIndexesProgressCommand;
