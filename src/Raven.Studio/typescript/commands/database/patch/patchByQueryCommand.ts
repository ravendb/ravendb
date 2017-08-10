import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class patchByQueryCommand extends commandBase {

    constructor(private queryStr: string, private patchRequest: Raven.Server.Documents.Patch.PatchRequest, private db: database) {
        super();
    }

    execute(): JQueryPromise<operationIdDto> {
        const url = endpoints.databases.queries.queries;
        const urlParams = "?allowStale=true";
        
        const payload = {
            Patch: this.patchRequest, 
            Query: {
                Query: this.queryStr
            }
        };
        
        return this.patch(url + urlParams, JSON.stringify(payload), this.db)
            .done((response: operationIdDto) => {
                this.reportSuccess("Scheduled patch based on query");
            })
            .fail((response: JQueryXHR) => this.reportError("Failed to schedule patch", response.responseText, response.statusText));
    }

}

export = patchByQueryCommand; 
