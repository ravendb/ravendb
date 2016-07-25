import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import pagedResultSet = require("common/pagedResultSet");
import facet = require("models/database/query/facet");
import document = require("models/database/documents/document");

class queryFacetsCommand extends commandBase {
    constructor(private indexName: string, private queryText: string, private skip: number, private take: number, private facets: facetDto[], private db: database, private disableCache: boolean = false) {
        super();
        this.argsUrl = this.urlEncodeArgs({
            query: this.queryText ? this.queryText : undefined,
            facetStart: this.skip,
            facetPageSize: this.take,
            disableCache: this.disableCache ? Date.now() : undefined,
            facets: JSON.stringify(this.facets)
        });
    }

    public argsUrl: string;

    execute(): JQueryPromise<pagedResultSet<any>> {
        var url = "/facets/" + this.indexName + this.argsUrl;

        // Querying facets returns a facetResultSetDto. We need to massage that
        // data into something that can be displayed in the grid: the pagedResultSet.
        var finishedTask = $.Deferred<pagedResultSet<any>>(); 
        this.query(url, null, this.db)
            .fail((response: JQueryXHR) => {
                this.reportError("Unable to run query.", response.responseText, response.statusText);
                finishedTask.reject(response);
            })
            .done((results: facetResultSetDto) => {
                finishedTask.resolve(this.parseResults(results));
            });

        return finishedTask;
    }

    private parseResults(resultSet: facetResultSetDto): pagedResultSet<any> {
        var items: dictionary<any>[] = [];
        var totalItemCount = 0;

        // The .Results property contains properties in the form of [facet name]-[facet aggregation field].
        // For example: Company-Total
        // Each of these properties will be of type facetResultDto.
        // In the UI, we display these as "[facet aggregation] of [facet aggregation field]", e.g. "Count of Total".
        var propIndex = 0;
        for (var prop in resultSet.Results) {
            var facetResult: facetResultDto = resultSet.Results[prop];
            var propNameParts = (<string>prop).split('-');
            var aggregateField = propNameParts[1];
            var remainingTerms = facetResult.RemainingTermsCount || 0;
            totalItemCount = facetResult.Values.length + remainingTerms + this.skip;

            // Construct our result set, an array objects that take the following shape:
            // { 'Key': [facet range], 'Count of Foobar': 3, 'Min of Blah': 4, ... }
            for (var i = 0; i < facetResult.Values.length; i++) {
                var facetValue = facetResult.Values[i];
                var item = items[i];
                if (!item) {
                    item = new document({});
                    items[i] = item;
                }

                item['Key'] = facetValue.Range;
                
                for (var power = 0; power < 5; power++) {
                    var curFieldName = facet.getLabelForAggregation(Math.pow(2, power));
                    var curFieldVal = (<any>facetValue)[curFieldName];

                    if (!!curFieldVal) {
                        if (typeof curFieldVal === "number") {
                            var fixedVal = curFieldVal.toFixed(2);
                            if (fixedVal != curFieldVal) {
                                curFieldVal = fixedVal;
                            }
                        }
                        item[curFieldName  + " of " + aggregateField] = curFieldVal;
                    }
                }
            }

            propIndex++;
        }

        return new pagedResultSet(items, totalItemCount, resultSet.Duration);
    }
}

export = queryFacetsCommand;
