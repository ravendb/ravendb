import commandBase = require("commands/commandBase");
import collectionInfo = require("models/database/documents/collectionInfo");
import collection = require("models/database/documents/collection");
import pagedResultSet = require("common/pagedResultSet");
import queryUtil = require("common/queryUtil");


class getDocumentsByEntityNameCommand extends commandBase {

    constructor(private collection: collection, private skip: number, private take: number) {
        super();
    }

    execute(): JQueryPromise<pagedResultSet> {
        var args = {
            query: "Tag:" + queryUtil.escapeTerm(this.collection.name),
            start: this.skip,
            pageSize: this.take,
            sort: "-LastModifiedTicks"
        };

        var resultsSelector = (dto: collectionInfoDto) => new collectionInfo(dto);
        var url =  "/indexes/Raven/DocumentsByEntityName";
        var documentsTask = $.Deferred();
        this.query(url, args, this.collection.ownerDatabase, resultsSelector)
            .fail(response => {
                if (response.status == ResponseCodes.InternalServerError) {
                    // old style index, probably, try again without the LastModifiedTicks
                    args.sort = "-LastModified";
                    this.query(url, args, this.collection.ownerDatabase, resultsSelector)
                        .fail(_ => documentsTask.reject())
                        .then(collection => {
                            var items = collection.results;
                            var resultSet = new pagedResultSet(items, collection.totalResults);
                            documentsTask.resolve(resultSet);
                        });
                    return;
                }
                documentsTask.reject()
            })
            .then(collection => {
                var items = collection.results;
                var resultSet = new pagedResultSet(items, collection.totalResults);
                documentsTask.resolve(resultSet);
            });
        return documentsTask;
    }
}

export = getDocumentsByEntityNameCommand;
