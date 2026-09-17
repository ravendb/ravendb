import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class getIndexesProgressCommand extends commandBase {

    private db: database | string;

    private location: databaseLocationSpecifier;

    private indexNames: string[];

    private exact: boolean;

    constructor(db: database | string, location: databaseLocationSpecifier, indexNames: string[] = null, exact = false) {
        super();
        this.location = location;
        this.db = db;
        this.indexNames = indexNames;
        this.exact = exact;
    }

    execute(): JQueryPromise<Raven.Client.Documents.Indexes.IndexProgress[]> {
        const args = {
            ...this.location,
            name: this.indexNames?.length ? this.indexNames : undefined,
            exact: this.exact ? true : undefined,
        };
        const url = endpoints.databases.index.indexesProgress + this.urlEncodeArgs(args);
        const extractor = (response: resultsDto<Raven.Client.Documents.Indexes.IndexProgress>) => response.Results;
        return this.query(url, null, this.db, extractor);
    }
}

export = getIndexesProgressCommand;
