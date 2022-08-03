import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class getIndexesProgressCommand extends commandBase {

    constructor(private db: database, private location: databaseLocationSpecifier) {
        super();
    }

    execute(): JQueryPromise<Raven.Client.Documents.Indexes.IndexProgress[]> {
        const url = endpoints.databases.index.indexesProgress;
        const args = {
            ...this.location
        }
        const extractor = (response: resultsDto<Raven.Client.Documents.Indexes.IndexProgress>) => response.Results;
        return this.query(url, args, this.db, extractor);
    }
} 

export = getIndexesProgressCommand;
