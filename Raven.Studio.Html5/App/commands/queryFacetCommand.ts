import commandBase = require("commands/commandBase");
import database = require("models/database");

class queryFacetCommand extends commandBase {
    constructor(private indexName: string, private queryText: string, private skip: number, private take: number, private facets: facetDto[], private db: database) {
        super();
    }

    execute(): JQueryPromise<{ ETag: string }> {
        var argsUrl = this.urlEncodeArgs({
            query: this.queryText,
            facetStart: this.skip,
            facetPageSize: this.take,
            facets: JSON.stringify(this.facets)
        });
        var url = "/facets/" + this.indexName + argsUrl;
        return this.query(url, null, this.db);
    }
} 