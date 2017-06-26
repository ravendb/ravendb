import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");
import document = require("models/database/documents/document");

class getRevisionsBinEntryCommand extends commandBase {

    constructor(private database: database, private etag: number, private take: number) {
        super();
    }

    execute(): JQueryPromise<pagedResult<document>> {
        const args = {
            etag: this.etag,
            pageSize: this.take
        };

        const resultsSelector = (dto: resultsDto<documentDto>, xhr: JQueryXHR) => {
            return {
                items: dto.Results.map(x => new document(x)),
                totalResultCount: -1,
                resultEtag: this.extractEtag(xhr)
            } as pagedResult<document>;
        };
        const url = endpoints.databases.versioning.revisionsBin + this.urlEncodeArgs(args);
        return this.query(url, null, this.database, resultsSelector);
    }

}

export = getRevisionsBinEntryCommand;
