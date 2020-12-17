import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import document = require("models/database/documents/document");
import endpoints = require("endpoints");
import queryCriteria = require("models/database/query/queryCriteria");
import queryUtil = require("common/queryUtil");

class queryCommand extends commandBase {

    private static readonly missingEndOfQuery = "Expected end of query"; 
    
    constructor(private db: database, private skip: number, private take: number, private criteria: queryCriteria, private disableCache?: boolean) {
        super();
    }

    execute(): JQueryPromise<pagedResultExtended<document>> {
        const selector = (results: Raven.Client.Documents.Queries.QueryResult<Array<any>, any>) =>
            ({
                items: results.Results.map(d => new document(d)), 
                totalResultCount: results.CappedMaxResults || results.TotalResults, 
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
                    const responseText = response.responseText;
                    let errorTitle = "Error querying index";

                    if (responseText.includes(queryCommand.missingEndOfQuery)) {
                        errorTitle = "Incorrect query command syntax";
                    }
                    
                    this.reportError(errorTitle, responseText, response.statusText);
                }
            });
    }

    private getQueryText() {
        const queryText = this.criteria.queryText();
        if (!queryText) {
            return [undefined, undefined];
        }

        let [parameters, rql] = queryCommand.extractQueryParameters(queryText);

        if (this.criteria.showFields()) {
            rql = queryUtil.replaceSelectAndIncludeWithFetchAllStoredFields(rql);
        } 
        
        return [parameters, rql];
    }

    static extractQueryParameters(queryText: string) {
        const arrayOfLines = queryText.match(/[^\r\n]+/g);
        if (arrayOfLines.length > 0) {
            let index = arrayOfLines.length - 1;
            let line = arrayOfLines[index].trim();
            while (line.endsWith("}") && !line.startsWith("{")) {
                if (!index) {
                    break;
                }
                    
                line = arrayOfLines[--index].trim() + " " + line;
            }
            if (line.endsWith("}") && line.startsWith("{")) {
                try {
                    // check if this is valid JSON, if so, can send it
                    JSON.parse(line);
                    const q = arrayOfLines.splice(0, index).join("\n");
                    return [line, q];
                } catch (e){
                    // ignore non JSON data
                }
            }
        }

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
            diagnostics: this.criteria.diagnostics() ? "true" : undefined,
            debug: criteria.indexEntries() ? "entries" : undefined,
            disableCache: this.disableCache ? Date.now() : undefined,
            addTimeSeriesNames: true,
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
