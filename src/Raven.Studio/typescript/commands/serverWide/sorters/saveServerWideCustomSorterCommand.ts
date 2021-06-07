import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class saveServerWideCustomSorterCommand extends commandBase {
    
    constructor(private sorterDto: Raven.Client.Documents.Queries.Sorting.SorterDefinition) {
        super();
    }

    execute(): JQueryPromise<void> {
        const url = endpoints.global.adminSorters.adminSorters;
        
        const payload = {
            Sorters: [ this.sorterDto ]
        };

        return this.put<void>(url, JSON.stringify(payload), null, { dataType: undefined })
            .done(() => this.reportSuccess(`Saved Server-Wide Custom Sorter ${this.sorterDto.Name}`))
            .fail((response: JQueryXHR) => this.reportError("Failed to save Server-Wide Custom Sorter", response.responseText, response.statusText));
    }
}

export = saveServerWideCustomSorterCommand;
