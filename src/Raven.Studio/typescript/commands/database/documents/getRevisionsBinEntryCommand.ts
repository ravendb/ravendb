import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");
import document = require("models/database/documents/document");

class getRevisionsBinEntryCommand extends commandBase {

    constructor(private database: database, private changeVector: string, private take: number) {
        super();
    }

    execute(): JQueryPromise<pagedResult<document>> {
        const args = {
            changeVector: this.changeVector,
            pageSize: this.take
        };

        const resultsSelector = (dto: resultsDto<documentDto>, xhr: JQueryXHR) => {
            return {
                items: dto.Results.map(x => new document(x)),
                totalResultCount: -1,
                resultEtag: this.extractEtag(xhr)
            } as pagedResult<document>;
        };
        const url = endpoints.databases.revisions.revisionsBin + this.urlEncodeArgs(args);
        return this.query(url, null, this.database, resultsSelector);
    }

}

export = getRevisionsBinEntryCommand;
