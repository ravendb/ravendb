import commandBase = require("commands/commandBase");
import database = require("models/resources/database");

class getDocumentsLeftToReplicate extends commandBase {

    constructor(private sourceUrl: string, private destinationUrl: string,
                private databaseName: string, private sourceId: string, private db: database) {
        super();
    }

    execute(): JQueryPromise<documentCountDto> {
        var url = "/admin/replication/docs-left-to-replicate";
        var args = {
            SourceUrl: this.sourceUrl,
            DestinationUrl: this.destinationUrl,
            DatabaseName: this.databaseName,
            SourceId: this.sourceId
        };
        
        return this.post(url, JSON.stringify(args), this.db, { dataType: "json" })
            .fail((response: JQueryXHR) => this.reportError("Failed to get documents left to replicate!", response.responseText, response.statusText));
    }
}

export = getDocumentsLeftToReplicate;
