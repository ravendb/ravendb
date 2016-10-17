import commandBase = require("commands/commandBase");
import collectionInfo = require("models/database/documents/collectionInfo");
import pagedResultSet = require("common/pagedResultSet");
import database = require("models/resources/database");

class getDocumentsPreviewCommand extends commandBase {

    constructor(private database: database, private skip: number, private take: number, private collectionName?: string, private bindings?: string[]) {
        super();
    }

    execute(): JQueryPromise<pagedResultSet> {
        var query = this.doQuery()
                        .fail(() => this.createSystemIndexAndTryAgain());
        return query;
    }

    doQuery() {
        var resultsSelector = (dto: documentPreviewDto) => {
            var collection = new collectionInfo(dto);
            var items = collection.results;
            return new pagedResultSet(items, collection.totalResults);
        };

        var args = {
            collection: this.collectionName,
            start: this.skip,
            pageSize: this.take,
            binding: this.bindings
        };

        var url = "/doc-preview";
        return this.query(url, args, this.database, resultsSelector);
    }

    createSystemIndexAndTryAgain() {
        // Calling silverlight/ensureStartup creates/updates the system index.
        this.query("/silverlight/ensureStartup", null, this.database)
            .done(() =>
            {
                this.doQuery();
            })
            .fail((response: JQueryXHR) => {
                this.reportError("Failed to get documents preview", response.responseText, response.statusText);
            });
    }
}

export = getDocumentsPreviewCommand;
