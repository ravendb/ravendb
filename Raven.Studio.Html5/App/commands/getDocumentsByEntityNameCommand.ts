import commandBase = require("commands/commandBase");
import database = require("models/database");
import collectionInfo = require("models/collectionInfo");
import collection = require("models/collection");
import pagedResultSet = require("common/pagedResultSet");

class getDocumentsByEntityNameCommand extends commandBase {

    constructor(private collection: collection, private skip: number, private take: number) {
        super();
    }

    execute(): JQueryPromise<pagedResultSet> {
        var args = {
            query: "Tag:" + this.collection.name,
            start: this.skip,
            pageSize: this.take
        };

        var resultsSelector = (dto: collectionInfoDto) => new collectionInfo(dto);
        var url =  "/indexes/Raven/DocumentsByEntityName";
        var documentsTask = $.Deferred();
        this.query(url, args, this.collection.ownerDatabase, resultsSelector)
            .then(collection => {
                var items = collection.results;
                var resultSet = new pagedResultSet(items, collection.totalResults);
                documentsTask.resolve(resultSet);
            });
        return documentsTask;
    }
}

export = getDocumentsByEntityNameCommand;