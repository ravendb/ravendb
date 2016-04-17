import pagedList = require("common/pagedList");
import getDocumentsPreviewCommand = require("commands/database/documents/getDocumentsPreviewCommand");
import getSystemDocumentsCommand = require("commands/database/documents/getSystemDocumentsCommand");
import pagedResultSet = require("common/pagedResultSet");
import database = require("models/resources/database");
import cssGenerator = require("common/cssGenerator");

class collection implements ICollectionBase {
    colorClass = ""; 
    documentCount: any = ko.observable(0);
    documentsCountWithThousandsSeparator = ko.computed(() => this.documentCount().toLocaleString());
    isAllDocuments = false;
    isSystemDocuments = false;
    bindings = ko.observable<string[]>();

    public collectionName : string;
    private documentsList: pagedList;
    public static allDocsCollectionName = "All Documents";
    private static systemDocsCollectionName = "System Documents";
    private static collectionColorMaps: resourceStyleMap[] = [];

    constructor(public name: string, public ownerDatabase: database, docCount: number = 0) {
        this.collectionName = name;
        this.isAllDocuments = name === collection.allDocsCollectionName;
        this.isSystemDocuments = name === collection.systemDocsCollectionName;
        this.colorClass = collection.getCollectionCssClass(name, ownerDatabase);
        this.documentCount(docCount);
    }

    // Notifies consumers that this collection should be the selected one.
    // Called from the UI when a user clicks a collection the documents page.
    activate() {
        ko.postbox.publish("ActivateCollection", this);
    }

    prettyLabel(text: string) {
        return text.replace(/__/g, '/');
    }
    getDocuments(): pagedList {
        if (!this.documentsList) {
            this.documentsList = this.createPagedList();
        }

        return this.documentsList;
    }

    invalidateCache() {
        var documentsList = this.getDocuments();
        documentsList.invalidateCache();
    }

    clearCollection() {
        if (this.isAllDocuments && !!this.documentsList) {
            this.documentsList.clear();
        }
    }

    fetchDocuments(skip: number, take: number): JQueryPromise<pagedResultSet> {
        if (this.isSystemDocuments) {
            // System documents don't follow the normal paging rules. See getSystemDocumentsCommand.execute() for more info.
            return new getSystemDocumentsCommand(this.ownerDatabase, skip, take, this.documentCount()).execute();
        } if (this.isAllDocuments) {
            return new getDocumentsPreviewCommand(this.ownerDatabase, skip, take, undefined, this.bindings()).execute();
        } else {
            return new getDocumentsPreviewCommand(this.ownerDatabase, skip, take, this.name, this.bindings()).execute();
        }
    }

    static createSystemDocsCollection(ownerDatabase: database): collection {
        return new collection(collection.systemDocsCollectionName, ownerDatabase);
    }

    static createAllDocsCollection(ownerDatabase: database): collection {
        return new collection(collection.allDocsCollectionName, ownerDatabase);
    }

    static getCollectionCssClass(entityName: string, db: database): string {
        if (entityName === collection.allDocsCollectionName) {
            return "all-documents-collection";
        }

        if (!entityName || entityName === collection.systemDocsCollectionName) {
            return "system-documents-collection";
        }

        return cssGenerator.getCssClass(entityName, collection.collectionColorMaps, db);
    }

    private createPagedList(): pagedList {
        var fetcher = (skip: number, take: number) => this.fetchDocuments(skip, take);
        var list = new pagedList(fetcher);
        list.collectionName = this.name;
        return list;
    }
}

export = collection;
