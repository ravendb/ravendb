import pagedList = require("common/pagedList");
import getCollectionInfoCommand = require("commands/getCollectionInfoCommand");
import getDocumentsByEntityNameCommand = require("commands/getDocumentsByEntityNameCommand");
import getSystemDocumentsCommand = require("commands/getSystemDocumentsCommand");
import getAllDocumentsCommand = require("commands/getAllDocumentsCommand");
import collectionInfo = require("models/collectionInfo");
import pagedResultSet = require("common/pagedResultSet");
import database = require("models/database");

class collection {

    colorClass = ""; 
    documentCount: any = ko.observable(0);
    documentsCountWithThousandsSeparator = ko.computed(() => this.documentCount().toLocaleString());
    isAllDocuments = false;
    isSystemDocuments = false;

    private documentsList: pagedList;
    public static allDocsCollectionName = "All Documents";
    private static systemDocsCollectionName = "System Documents";
    private static collectionColorMaps: databaseCollectionStyleMap[] = [];

    constructor(public name: string, public ownerDatabase: database, docCount: number = 0) {
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

    clearCollection() {
        if (this.isAllDocuments === true && !!this.documentsList) {
            this.documentsList.clear();
        }
    }

    fetchDocuments(skip: number, take: number): JQueryPromise<pagedResultSet> {
        if (this.isSystemDocuments) {
            // System documents don't follow the normal paging rules. See getSystemDocumentsCommand.execute() for more info.
            var task = new getSystemDocumentsCommand(this.ownerDatabase, skip, take).execute();
            task.done((results: pagedResultSet) => this.documentCount(results.totalResultCount));
            return task;
        } if (this.isAllDocuments) {
            return new getAllDocumentsCommand(this.ownerDatabase, skip, take).execute();
        } else {
            return new getDocumentsByEntityNameCommand(this, skip, take).execute();
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

        var databaseStyleMap = this.collectionColorMaps.first(map => map.databaseName == db.name);
        if (!databaseStyleMap) {
            databaseStyleMap = {
                databaseName: db.name,
                styleMap: {}
            };
            this.collectionColorMaps.push(databaseStyleMap);
        }

        var existingStyle = databaseStyleMap.styleMap[entityName];
        if (existingStyle) {
            return existingStyle;
        } 

        // We don't have an existing style. Assign one in the form of 'collection-style-X', where X is a number between 0 and maxStyleCount. These styles are found in app.less.
        var maxStyleCount = 32;
        var styleNumber = Object.keys(databaseStyleMap.styleMap).length % maxStyleCount;
        var style = "collection-style-" + styleNumber;
        databaseStyleMap.styleMap[entityName] = style;
        return style;
    }

    private createPagedList(): pagedList {
        var fetcher = (skip: number, take: number) => this.fetchDocuments(skip, take);
        var list = new pagedList(fetcher);
        list.collectionName = this.name;
        return list;
    }
}

export = collection;
