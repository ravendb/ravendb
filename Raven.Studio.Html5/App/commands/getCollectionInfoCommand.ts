import commandBase = require("commands/commandBase");
import database = require("models/database");
import collectionInfo = require("models/collectionInfo");
import collection = require("models/collection");

class getCollectionInfoCommand extends commandBase {

    constructor(private collection: collection) {
        super();
    }

    execute(): JQueryPromise<collectionInfo> {
        var args = {
            query: "Tag:" + (this.collection.isAllDocuments ? '' : this.collection.name),
            start: 0,
            pageSize: 0
        };

        var resultsSelector = (dto: collectionInfoDto) => new collectionInfo(dto);
        var url =  "/indexes/Raven/DocumentsByEntityName";
        return this.query(url, args, this.collection.ownerDatabase, resultsSelector);
    }
}

export = getCollectionInfoCommand;