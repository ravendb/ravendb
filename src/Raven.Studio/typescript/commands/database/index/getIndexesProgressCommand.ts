import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class getIndexesProgressCommand extends commandBase {

    private db: database;

    private location: databaseLocationSpecifier;

    constructor(db: database, location: databaseLocationSpecifier) {
        super();
        this.location = location;
        this.db = db;
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
