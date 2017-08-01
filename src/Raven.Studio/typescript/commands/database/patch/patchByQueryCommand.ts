import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class patchByQueryCommand extends commandBase {

    constructor(private indexName: string, private queryStr: string, private patchRequest: Raven.Server.Documents.Patch.PatchRequest, private db: database) {
        super();
    }

    execute(): JQueryPromise<operationIdDto> {
        const url = endpoints.databases.queries.queries + this.indexName;
        const urlParams = "?query=" + encodeURIComponent(this.queryStr) + "&allowStale=true";
        return this.patch(url + urlParams, JSON.stringify(this.patchRequest), this.db)
            .done((response: operationIdDto) => {
                this.reportSuccess("Scheduled patch of index: " + this.indexName);
            })
            .fail((response: JQueryXHR) => this.reportError("Failed to schedule patch of index " + this.indexName, response.responseText, response.statusText));
    }

}

export = patchByQueryCommand; 
