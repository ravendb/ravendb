import database = require("models/resources/database");
import document = require("models/database/documents/document");
import getCollectionFieldsCommand = require("commands/database/documents/getCollectionFieldsCommand");
import getDocumentsPreviewCommand = require("commands/database/documents/getDocumentsPreviewCommand");
import generalUtils = require("common/generalUtils");

class collection {
    static readonly allDocumentsCollectionName = "All Documents";
    static readonly systemDocumentsCollectionName = "@system";
    static readonly revisionsBinCollectionName = "Revisions Bin";

    documentCount: KnockoutObservable<number> = ko.observable(0);
    name: string;
    sizeClass: KnockoutComputed<string>;
    countPrefix: KnockoutComputed<number>;
    hasBounceClass = ko.observable<boolean>(false);

    private db: database;

    constructor(name: string, ownerDatabase: database, docCount: number = 0) {
        this.name = name;
        this.db = ownerDatabase;
        this.documentCount(docCount);

        this.sizeClass = ko.pureComputed(() => {
            return generalUtils.getSizeClass(this.documentCount());
        });

        this.countPrefix = ko.pureComputed(() => {
            return generalUtils.getCountPrefix(this.documentCount());
        });

        this.documentCount.subscribe(() => {
            if (this.hasBounceClass()) {
                return;
            }

            this.hasBounceClass(true);

            setTimeout(() => {
                this.hasBounceClass(false);
            }, 420);
        });
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

    fetchDocuments(skip: number, take: number, previewColumns?: string[], fullColumns?: string[]): JQueryPromise<pagedResultWithAvailableColumns<document>> {
        const collection = this.isAllDocuments ? undefined : this.name;
        return new getDocumentsPreviewCommand(this.db, skip, take, collection, previewColumns, fullColumns)
            .execute();
    }

    fetchFields(prefix: string): JQueryPromise<object> {
        const collection = this.isAllDocuments ? undefined : this.name;
        return new getCollectionFieldsCommand(this.db, collection, prefix)
            .execute();
    }

    static createAllDocumentsCollection(ownerDatabase: database, documentsCount: number): collection {
        return new collection(collection.allDocumentsCollectionName, ownerDatabase, documentsCount);
    }

}

export = collection;
