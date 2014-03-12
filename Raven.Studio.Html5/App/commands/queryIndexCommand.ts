import commandBase = require("commands/commandBase");
import database = require("models/database");
import document = require("models/document");
import pagedResultSet = require("common/pagedResultSet");
import querySort = require("models/querySort");

class queryIndexCommand extends commandBase {
    constructor(private indexName: string, private db: database, private skip: number, private take: number, private queryText?: string, private sorts?: querySort[], private transformerName?: string, private showFields?: boolean, private indexEntries?: boolean, private useAndOperator?: boolean) {
        super();
    }

    execute(): JQueryPromise<pagedResultSet> {
        

        var selector = (results: indexQueryResultsDto) => new pagedResultSet(results.Results.map(d => new document(d)), results.TotalResults, results);
        var queryTask = this.query(this.getUrl(), null, this.db, selector);
        queryTask.fail((response: JQueryXHR) => this.reportError("Error querying index", response.responseText, response.statusText));

        return queryTask;
    }

    getUrl() {
        var url = "/indexes/" + this.indexName;
        var resultsTransformerUrlFragment = this.transformerName ? "&resultsTransformer=" + this.transformerName : ""; // This should not be urlEncoded, as it breaks the query.
        var urlArgs = this.urlEncodeArgs({
            query: this.queryText ? this.queryText : undefined,
            start: this.skip,
            pageSize: this.take,
            sort: this.sorts.map(s => s.toQuerySortString()),
            skipTransformResults: true,
            fetch: this.showFields ? "__all_fields" : undefined,
            debug: this.indexEntries ? "entries" : undefined,
            operator: this.useAndOperator ? "AND" : undefined
        }) + resultsTransformerUrlFragment;

        return url + urlArgs;
    }
}

export = queryIndexCommand;