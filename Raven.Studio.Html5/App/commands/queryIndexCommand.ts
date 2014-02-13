import commandBase = require("commands/commandBase");
import database = require("models/database");
import document = require("models/document");
import pagedResultSet = require("common/pagedResultSet");

class queryIndexCommand extends commandBase {
    constructor(private indexName: string, private db: database, private skip: number, private take: number, private queryText?: string) {
        super();
    }

    execute(): JQueryPromise<pagedResultSet> {
        var url = "/indexes/" + this.indexName;
        var urlArgs = this.urlEncodeArgs({
            query: this.queryText,
            start: this.skip,
            pageSize: this.take 
        });

        var selector = (results: indexQueryResultsDto) => new pagedResultSet(results.Results.map(d => new document(d)), results.TotalResults, results);
        return this.query(url + urlArgs, null, this.db, selector);
    }
}

export = queryIndexCommand;