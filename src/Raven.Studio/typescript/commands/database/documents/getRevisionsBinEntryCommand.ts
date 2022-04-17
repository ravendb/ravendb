import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");
import document = require("models/database/documents/document");

class getRevisionsBinEntryCommand extends commandBase {

    constructor(private database: database, private changeVector: string, private take: number, private continuationToken?: string) {
        super();
    }
    
    execute(): JQueryPromise<pagedResultWithToken<document>> {
        const args = this.getArgsToUse();

        const resultsSelector = (dto: resultsWithCountAndToken<documentDto>, xhr: JQueryXHR): pagedResultWithToken<document> => {
            return {
                items: dto.Results.map(x => new document(x)),
                totalResultCount: -1,
                resultEtag: this.extractEtag(xhr),
                continuationToken: dto.ContinuationToken
            };
        };
        
        const url = endpoints.databases.revisions.revisionsBin + this.urlEncodeArgs(args);
        
        return this.query(url, null, this.database, resultsSelector)
            .fail((response: JQueryXHR) => this.reportError("Failed to get revision bin entries", response.responseText, response.statusText));
    }

    private getArgsToUse() {
        if (this.continuationToken) {
            return {
                continuationToken: this.continuationToken
            };
        }

        return {
            changeVector: this.changeVector,
            pageSize: this.take
        };
    }
}

export = getRevisionsBinEntryCommand;
