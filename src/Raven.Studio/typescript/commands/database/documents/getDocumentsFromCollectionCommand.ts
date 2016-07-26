import commandBase = require("commands/commandBase");
import collection = require("models/database/documents/collection");
import pagedResultSet = require("common/pagedResultSet");
import document = require("models/database/documents/document");
import endpoints = require("endpoints");

class getDocumentsFromCollectionCommand extends commandBase {

    constructor(private collection: collection, private skip: number, private take: number) {
        super();
    }

    execute(): JQueryPromise<pagedResultSet<document>> {
        var args = {
            name: this.collection.name,
            start: this.skip,
            pageSize: this.take
        };

        var resultsSelector = (dto: any[]) => new pagedResultSet(dto.map(x => new document(x)), this.collection.documentCount());
        var url = endpoints.databases.collections.collectionsDocs;

        return this.query(url, args, this.collection.ownerDatabase, resultsSelector);
    }
}

export = getDocumentsFromCollectionCommand;
