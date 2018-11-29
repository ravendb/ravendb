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

    execute(): JQueryPromise<pagedResultExtended<document>> {
        const selector = (results: Raven.Client.Documents.Queries.QueryResult<Array<any>, any>) =>
            ({
                items: results.Results.map(d => new document(d)), 
                totalResultCount: results.TotalResultsWithOffsetAndLimit || results.TotalResults, 
                additionalResultInfo: results, 
                resultEtag: results.ResultEtag.toString(), 
                highlightings: results.Highlightings,
                explanations: results.Explanations,
                timings: results.Timings,
                includes: results.Includes }) as pagedResultExtended<document>;
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
        const queryText = this.criteria.queryText();
        if (!queryText) {
            return [undefined, undefined];
        }

        let [parameters, rql] = this.extractQueryParameters(queryText);

        if (this.criteria.showFields()) {
            rql = queryUtil.replaceSelectAndIncludeWithFetchAllStoredFields(rql);
        } 
        
        return [parameters, rql];
    }

    private extractQueryParameters(queryText: string) {
        const parametersEndRegex = /^\s*(with|match|from|declare)/mi;
        const match = parametersEndRegex.exec(queryText);
        if (!match) {
            return [undefined, queryText];
        }
        const parametersText = queryText.substring(0, match.index);
        const params = parametersText.replace(/(?:^|;\s*)[$]/gm, "result.");
        const parametersJs = `
var f = function() {
    var result = {};
    ${params}
    return JSON.stringify(result);
}
f();
`;
        let parameters = eval(parametersJs);
        const rql = queryText.substring(match.index);
        return [parameters, rql];
    }
    
    getUrl() {
        const criteria = this.criteria;
        const url = endpoints.databases.queries.queries;
        const [parameters, rql] = this.getQueryText();
        
        const urlArgs = this.urlEncodeArgs({
            query: rql,
            parameters: parameters,
            start: this.skip,
            pageSize: this.take,
            debug: criteria.indexEntries() ? "entries" : undefined,
            disableCache: this.disableCache ? Date.now() : undefined,
            metadataOnly: typeof(criteria.metadataOnly()) !== 'undefined' ? criteria.metadataOnly() : undefined,
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
