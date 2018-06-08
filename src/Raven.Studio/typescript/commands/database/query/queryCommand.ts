import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import document = require("models/database/documents/document");
import endpoints = require("endpoints");
import queryCriteria = require("models/database/query/queryCriteria");
import queryUtil = require("common/queryUtil");

class queryCommand extends commandBase {
    constructor(private db: database, private skip: number, private take: number, private criteria: queryCriteria, private disableCache?: boolean) {
        super();
    }

    execute(): JQueryPromise<pagedResultWithIncludes<document>> {
        const selector = (results: Raven.Client.Documents.Queries.QueryResult<Array<any>, any>) =>
            ({ items: results.Results.map(d => new document(d)), totalResultCount: results.TotalResults, additionalResultInfo: results, resultEtag: results.ResultEtag.toString(), includes: results.Includes }) as pagedResultWithIncludes<document>;
        return this.query(this.getUrl(), null, this.db, selector)
            .fail((response: JQueryXHR) => {
                if (response.status === 404) {
                    this.reportError("Error querying index", "Index was not found", response.statusText)
                } else {
                    this.reportError("Error querying index", response.responseText, response.statusText)
                }
            });
    }

    private getQueryText() {
        if (!this.criteria.queryText()) {
            return undefined;
        }
        
        if (this.criteria.showFields()) {
            return queryUtil.replaceSelectAndIncludeWithFetchAllStoredFields(this.criteria.queryText());
        } else {
            return this.criteria.queryText();
        }
    }
    
    getUrl() {
        const criteria = this.criteria;
        const url = endpoints.databases.queries.queries;

        const urlArgs = this.urlEncodeArgs({
            query: this.getQueryText(),
            start: this.skip,
            pageSize: this.take,
            debug: criteria.indexEntries() ? "entries" : undefined,
            disableCache: this.disableCache ? Date.now() : undefined,
            metadataOnly: typeof(criteria.metadataOnly()) !== 'undefined' ? criteria.metadataOnly() : undefined
        });
        return url + urlArgs;
    }

    getCsvUrl() {
        const criteria = this.criteria;

        const url = endpoints.databases.streaming.streamsQueries;
        
        const urlArgs = this.urlEncodeArgs({
            query: this.getQueryText(),
            debug: criteria.indexEntries() ? "entries" : undefined,
            format: "excel",
            download: true
        });

        return url + urlArgs;
    }
}

export = queryCommand;
