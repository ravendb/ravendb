import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class getCustomSortersCommand extends commandBase {

    constructor(private db: database) {
        super();
    }

    execute(): JQueryPromise<Array<Raven.Client.Documents.Queries.Sorting.SorterDefinition>> {
        const url = endpoints.databases.sorters.sorters;
        return this.query<Array<Raven.Client.Documents.Queries.Sorting.SorterDefinition>>(url, null, this.db, x => x.Sorters)
            .fail((response: JQueryXHR) => {
                this.reportError("Failed to get custom sorters", response.responseText, response.statusText); 
            });
    }
}

export = getCustomSortersCommand; 

