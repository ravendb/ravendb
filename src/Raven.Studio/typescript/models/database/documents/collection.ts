import generalUtils = require("common/generalUtils");
import { Collection } from "components/common/shell/collectionsTrackerSlice";

class collection {
    static readonly allDocumentsCollectionName = "All Documents";
    static readonly allRevisionsCollectionName = "All Revisions";
    static readonly revisionsBinCollectionName = "Revisions Bin";
    static readonly hiloCollectionName = "@hilo";

    documentCount: KnockoutObservable<number> = ko.observable(0);
    lastDocumentChangeVector = ko.observable<string>();
    name: string;
    sizeClass: KnockoutComputed<string>;
    countPrefix: KnockoutComputed<string>;
    hasBounceClass = ko.observable<boolean>(false);

    constructor(name: string, docCount = 0) {
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

    get isAllRevisions() {
        return this.name === collection.allRevisionsCollectionName;
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

    toCollectionState(): Collection {
        return {
            name: this.name,
            countPrefix: this.countPrefix(),
            documentCount: this.documentCount(),
            hasBounceClass: this.hasBounceClass(),
            lastDocumentChangeVector: this.lastDocumentChangeVector(),
            sizeClass: this.sizeClass(),
        }
    }
}

export = collection;
