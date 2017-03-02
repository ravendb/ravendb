import getDocumentsFromCollectionCommand = require("commands/database/documents/getDocumentsFromCollectionCommand");
import getAllDocumentsMetadataCommand = require("commands/database/documents/getAllDocumentsMetadataCommand");
import database = require("models/resources/database");
import cssGenerator = require("common/cssGenerator");
import document = require("models/database/documents/document");

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
        //TODO: use doc-preview endpoint for fetching this!
        /*
        TODO:
        In current commands we use total results from collection count,
        as result it produces invalid output after deleting documents.
        Solution is to add total results count to response, but we anyway will implement
        doc-preview endpoint with this property, so let's leave it for now. 
        */
        if (this.isAllDocuments) {
            return new getAllDocumentsMetadataCommand(this.db, skip, take).execute();
        } else {
            return new getDocumentsFromCollectionCommand(this, skip, take).execute();
        }
    }

    static createAllDocumentsCollection(ownerDatabase: database, documentsCount: number): collection {
        return new collection(collection.allDocumentsCollectionName, ownerDatabase, documentsCount);
    }

}

export = collection;
