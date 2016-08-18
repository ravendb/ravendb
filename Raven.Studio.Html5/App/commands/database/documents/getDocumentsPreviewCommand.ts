import commandBase = require("commands/commandBase");
import collectionInfo = require("models/database/documents/collectionInfo");
import pagedResultSet = require("common/pagedResultSet");
import database = require("models/resources/database");

class getDocumentsPreviewCommand extends commandBase {

    constructor(private database: database, private skip: number, private take: number, private collectionName?: string, private bindings?: string[]) {
        super();
    }

    execute(): JQueryPromise<pagedResultSet> {
        var args = {
            collection: this.collectionName,
            start: this.skip,
            pageSize: this.take,
            binding: this.bindings
        };

        var resultsSelector = (dto: documentPreviewDto) => {
            var collection = new collectionInfo(dto);
            var items = collection.results;
            return new pagedResultSet(items, collection.totalResults);
        };
        var url = "/doc-preview";
        var query = this.query(url, args, this.database, resultsSelector);
        query.fail((response: JQueryXHR) => this.reportError("Failed to get documents preview", response.responseText, response.statusText));
        return query;
    }
}

export = getDocumentsPreviewCommand;
