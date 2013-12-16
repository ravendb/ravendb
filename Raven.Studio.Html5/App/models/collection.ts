import raven = require("common/raven");
import pagedList = require("common/pagedList");
import getCollectionInfoCommand = require("commands/getCollectionInfoCommand");
import getDocumentsCommand = require("commands/getDocumentsCommand");
import getSystemDocumentsCommand = require("commands/getSystemDocumentsCommand");
import getAllDocumentsCommand = require("commands/getAllDocumentsCommand");
import collectionInfo = require("models/collectionInfo");
import pagedResultSet = require("common/pagedResultSet");
import database = require("models/database");

class collection {

    colorClass = ""; 
    documentCount: any = ko.observable(0);
    isAllDocuments = false;
    isSystemDocuments = false;

    private documentsList: pagedList;
    private static allDocsCollectionName = "All Documents";
    private static systemDocsCollectionName = "System Documents";

    constructor(public name: string, public ownerDatabase: database) {
        this.isAllDocuments = name === collection.allDocsCollectionName;
        this.isSystemDocuments = name === collection.systemDocsCollectionName;
	}

	// Notifies consumers that this collection should be the selected one.
	// Called from the UI when a user clicks a collection the documents page.
	activate() {
		ko.postbox.publish("ActivateCollection", this);
    }

    getInfo(db: database) {
        new getCollectionInfoCommand(this, db)
            .execute()
            .done((info: collectionInfo) => this.documentCount(info.totalResults));
    }

    getDocuments(): pagedList {
        if (!this.documentsList) {
            this.documentsList = this.createPagedList();
        }

        return this.documentsList;
    }

    fetchDocuments(skip: number, take: number): JQueryPromise<pagedResultSet> {
        if (this.isSystemDocuments) {
            return new getSystemDocumentsCommand(this.ownerDatabase, skip, take).execute();
        } if (this.isAllDocuments) {
            return new getAllDocumentsCommand(this.ownerDatabase, skip, take).execute();
        } else {
            return new getDocumentsCommand(this, skip, take).execute();
        }
    }

    static createSystemDocsCollection(ownerDatabase: database): collection {
        return new collection(collection.systemDocsCollectionName, ownerDatabase);
    }

    static createAllDocsCollection(ownerDatabase: database): collection {
        return new collection(collection.allDocsCollectionName, ownerDatabase);
    }

    private createPagedList(): pagedList {
        var fetcher = (skip: number, take: number) => this.fetchDocuments(skip, take);
        var list = new pagedList(fetcher);
        list.collectionName = this.name;
        return list;
    }
}

export = collection;