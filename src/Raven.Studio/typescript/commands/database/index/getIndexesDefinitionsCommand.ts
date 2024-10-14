import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

interface GetIndexesDefinitionsRequestOptions {
    indexNames?: string[];
    skip?: number;
    take?: number;
}

class getIndexesDefinitionsCommand extends commandBase {
    private readonly db: database | string;
    private readonly options: GetIndexesDefinitionsRequestOptions;

    constructor(db: database | string, options: GetIndexesDefinitionsRequestOptions = {}) {
        super();
        this.db = db;
        this.options = options; // for empty options it will take all definitions
    }

    execute(): JQueryPromise<Array<Raven.Client.Documents.Indexes.IndexDefinition>> {
        const args = this.getArgs();
        const url = endpoints.databases.index.indexes + this.urlEncodeArgs(args);

        const extractor = (response: resultsDto<Raven.Client.Documents.Indexes.IndexDefinition>) => response.Results;

        return this.query(url, null, this.db, extractor);
    }

    private getArgs() {
        const { indexNames, skip, take } = this.options;

        if (indexNames) {
            return {
                // we pass the list to GET like: ?name=A&name=B...
                // for now we ignore the length limit (8kb)
                name: indexNames,
            };
        }

        return {
            start: skip,
            pageSize: take,
        };
    }
}

export = getIndexesDefinitionsCommand;
