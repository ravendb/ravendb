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
        const args = {
            name: this.collection.name,
            start: this.skip,
            pageSize: this.take
        };

        const resultsSelector = (dto: resultsDto<any>) => new pagedResultSet(dto.Results.map(x => new document(x)), this.collection.documentCount());
        const url = endpoints.databases.collections.collectionsDocs;

        return this.query(url, args, this.collection.database, resultsSelector);
    }
}

export = getDocumentsFromCollectionCommand;
