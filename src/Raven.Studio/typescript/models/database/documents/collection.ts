import database = require("models/resources/database");
import cssGenerator = require("common/cssGenerator");
import document = require("models/database/documents/document");
import getDocumentsPreviewCommand = require("commands/database/documents/getDocumentsPreviewCommand");

class collection {
    static readonly allDocumentsCollectionName = "All Documents";
    static readonly systemDocumentsCollectionName = "@system";

    documentCount: KnockoutObservable<number> = ko.observable(0);
    name: string;

    private db: database;

    constructor(name: string, ownerDatabase: database, docCount: number = 0) {
        this.name = name;
        this.db = ownerDatabase;
        this.documentCount(docCount);
    }

    get isSystemDocuments() {
        return this.name === collection.systemDocumentsCollectionName;
    }

    get isAllDocuments() {
        return this.name === collection.allDocumentsCollectionName;
    }

    get database() {
        return this.db;
    }

    fetchDocuments(skip: number, take: number): JQueryPromise<pagedResult<document>> {
        if (this.isAllDocuments) {
            return new getDocumentsPreviewCommand(this.db, skip, take)
                .execute(); //TODO:bindings
        } else {
            return new getDocumentsPreviewCommand(this.db, skip, take, this.name)
                .execute(); //TODO: bindings
        }
    }

    static createAllDocumentsCollection(ownerDatabase: database, documentsCount: number): collection {
        return new collection(collection.allDocumentsCollectionName, ownerDatabase, documentsCount);
    }

}

export = collection;
