import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class getIndexTermsCommand extends commandBase {

    constructor(private indexName: string, private collection: string, private field: string, private db: database, private pageSize: number, private fromValue: string = undefined) {
        super();
    }

    execute(): JQueryPromise<Raven.Client.Documents.Queries.TermsQueryResult> {
        const args = {
            field: this.field,
            name: this.indexName, 
            collection: this.collection, 
            pageSize: this.pageSize,
            fromValue: this.fromValue
        };
        const url = endpoints.databases.index.indexesTerms;
        return this.query(url, args, this.db);
    }
} 

export = getIndexTermsCommand;
