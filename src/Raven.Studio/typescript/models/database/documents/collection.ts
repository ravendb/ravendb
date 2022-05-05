import database = require("models/resources/database");
import generalUtils = require("common/generalUtils");

class collection {
    static readonly allDocumentsCollectionName = "All Documents";
    static readonly revisionsBinCollectionName = "Revisions Bin";
    static readonly hiloCollectionName = "@hilo";

    documentCount: KnockoutObservable<number> = ko.observable(0);
    name: string;
    sizeClass: KnockoutComputed<string>;
    countPrefix: KnockoutComputed<string>;
    hasBounceClass = ko.observable<boolean>(false);

    constructor(name: string, docCount: number = 0) {
        this.name = name;
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

    get isAllDocuments() {
        return this.name === collection.allDocumentsCollectionName;
    }
    
    get isRevisionsBin() {
        return this.name === collection.revisionsBinCollectionName;
    }

    get collectionNameForQuery() {
        return this.isAllDocuments ? "@all_docs" : this.name;
    }

    static createAllDocumentsCollection(documentsCount: number): collection {
        return new collection(collection.allDocumentsCollectionName, documentsCount);
    }

}

export = collection;
