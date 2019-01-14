import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class saveCustomSorterCommand extends commandBase {

    constructor(private db: database, private sorterDto: Raven.Client.Documents.Queries.Sorting.SorterDefinition) {
        super();
    }

    execute(): JQueryPromise<void> {
        const url = endpoints.databases.adminSorters.adminSorters;
        const payload = {
            Sorters: [ this.sorterDto ]
        };

        return this.put<void>(url, JSON.stringify(payload), this.db, { dataType: undefined })
            .fail((response: JQueryXHR) => {
                this.reportError("Failed to save custom sorter", response.responseText, response.statusText); 
            })
            .done(() => {
                this.reportSuccess(`Saved custom sorter ${this.sorterDto.Name}`);
            });
    }
}

export = saveCustomSorterCommand; 

