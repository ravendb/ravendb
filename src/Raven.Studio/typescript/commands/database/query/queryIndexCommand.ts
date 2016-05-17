import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import document = require("models/database/documents/document");
import pagedResultSet = require("common/pagedResultSet");
import querySort = require("models/database/query/querySort");
import transformerQueryType = require("models/database/index/transformerQuery");

class queryIndexCommand extends commandBase {
    constructor(private indexName: string, private db: database, private skip: number, private take: number, private queryText?: string, private sorts?: querySort[], private transformerQuery?: transformerQueryType,
        private showFields?: boolean, private indexEntries?: boolean, private useAndOperator?: boolean, private disableCache?: boolean) {
        super();
    }

    execute(): JQueryPromise<pagedResultSet<any>> {
        var selector = (results: indexQueryResultsDto) => new pagedResultSet(results.Results.map(d => new document(d)), results.TotalResults, results);
        var queryTask = this.query(this.getUrl(), null, this.db, selector);
        queryTask.fail((response: JQueryXHR) => this.reportError("Error querying index", response.responseText, response.statusText));

        return queryTask;
    }

    getUrl() {
        var url = "/queries/" + this.indexName;
        //var resultsTransformerUrlFragment = this.transformer && this.transformer.name() ? "&resultsTransformer=" + this.transformer.name() : ""; // This should not be urlEncoded, as it breaks the query.
        var resultsTransformerUrlFragment = (this.transformerQuery ? this.transformerQuery.toUrl() : "");
        var urlArgs = this.urlEncodeArgs({
            query: this.queryText ? this.queryText : undefined,
            start: this.skip,
            pageSize: this.take,
            sort: this.sorts.map(s => s.toQuerySortString()),
            fetch: this.showFields ? "__all_fields" : undefined,
            debug: this.indexEntries ? "entries" : undefined,
            operator: this.useAndOperator ? "AND" : undefined, 
            disableCache: this.disableCache ? Date.now() : undefined
        }) + resultsTransformerUrlFragment;
        return url + urlArgs;
    }

    getCsvUrl() {
        var url = "/streams/query/" + this.indexName;
        var resultsTransformerUrlFragment = (this.transformerQuery ? this.transformerQuery.toUrl() : "");
        var urlArgs = this.urlEncodeArgs({
            query: this.queryText ? this.queryText : undefined,
            sort: this.sorts.map(s => s.toQuerySortString()),
            fetch: this.showFields ? "__all_fields" : undefined,
            debug: this.indexEntries ? "entries" : undefined,
            operator: this.useAndOperator ? "AND" : undefined,
            format: "excel",
            download: true
        }) + resultsTransformerUrlFragment;

        return url + urlArgs;
    }
}

export = queryIndexCommand;
