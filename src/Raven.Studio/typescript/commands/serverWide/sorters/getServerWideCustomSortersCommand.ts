import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class getServerWideCustomSortersCommand extends commandBase {

    execute(): JQueryPromise<Array<Raven.Client.Documents.Queries.Sorting.SorterDefinition>> {
        const url = endpoints.global.sorters.sorters;
        
        return this.query<Array<Raven.Client.Documents.Queries.Sorting.SorterDefinition>>(url, null, null, x => x.Sorters)
            .fail((response: JQueryXHR) => {
                this.reportError("Failed to get Server-Wide Custom Sorters", response.responseText, response.statusText); 
            });
    }
}

export = getServerWideCustomSortersCommand;
