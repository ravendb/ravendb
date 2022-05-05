import commandBase = require("commands/commandBase");
import collection = require("models/database/documents/collection");
import document = require("models/database/documents/document");
import endpoints = require("endpoints");
import database from "models/resources/database";

class getDocumentsFromCollectionCommand extends commandBase {

    constructor(private collection: collection, private db: database, private skip: number, private take: number) {
        super();
    }

    execute(): JQueryPromise<pagedResult<document>> {
        const args = {
            name: this.collection.name,
            start: this.skip,
            pageSize: this.take
        };

        const resultsSelector = (dto: resultsDto<any>, xhr: JQueryXHR): pagedResult<document> => {
            return { items: dto.Results.map(x => new document(x)), totalResultCount: (dto as any).totalResultCount, resultEtag: this.extractEtag(xhr) };
        };
        const url = endpoints.databases.collections.collectionsDocs;

        return this.query(url, args, this.db, resultsSelector);
    }
}

export = getDocumentsFromCollectionCommand;
