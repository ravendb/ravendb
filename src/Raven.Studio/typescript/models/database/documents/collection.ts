import database = require("models/resources/database");
import document = require("models/database/documents/document");
import getDocumentsPreviewCommand = require("commands/database/documents/getDocumentsPreviewCommand");

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
            const count = this.documentCount();
            if (count < 100000) {
                return "";
            }
            if (count < 1000 * 1000) {
                return "kilo";
            }
            return "mega";
        });

        this.countPrefix = ko.pureComputed(() => {
            const count = this.documentCount();
            if (count < 100000) {
                return count;
            }
            if (count < 1000 * 1000) {
                return _.floor(count / 1000, 2);
            }
            return _.floor(count / 1000000, 2);
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
        if (this.isAllDocuments) {
            return new getDocumentsPreviewCommand(this.db, skip, take, undefined, previewColumns, fullColumns)
                .execute();
        } else {
            return new getDocumentsPreviewCommand(this.db, skip, take, this.name, previewColumns, fullColumns)
                .execute();
        }
    }

    static createAllDocumentsCollection(ownerDatabase: database, documentsCount: number): collection {
        return new collection(collection.allDocumentsCollectionName, ownerDatabase, documentsCount);
    }

}

export = collection;
