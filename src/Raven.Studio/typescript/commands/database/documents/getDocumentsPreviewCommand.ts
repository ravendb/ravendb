import commandBase = require("commands/commandBase");
import collectionInfo = require("models/database/documents/collectionInfo");
import pagedResultSet = require("common/pagedResultSet");
import database = require("models/resources/database");

class getDocumentsPreviewCommand extends commandBase {

    constructor(private database: database, private skip: number, private take: number, private collectionName?: string, private bindings?: string[]) {
        super();
    }
    /*TODO: 
    execute(): JQueryPromise<pagedResultSet<any>> {
        var args = {
            collection: this.collectionName,
            start: this.skip,
            pageSize: this.take,
            binding: this.bindings
        };

        var resultsSelector = (dto: documentPreviewDto) => {
            var collection = new collectionInfo(null);
            var items = collection.results;
            return new pagedResultSet(items, collection.totalResults);
        };
        var url = "/doc-preview";//TODO: use endpoints
        return this.query(url, args, this.database, resultsSelector);
    }*/
}

export = getDocumentsPreviewCommand;
