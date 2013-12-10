import raven = require("common/raven");
import pagedList = require("common/pagedList");
import getCollectionInfoCommand = require("commands/getCollectionInfoCommand");
import collectionInfo = require("models/collectionInfo");
import database = require("models/database");

class collection {

    colorClass = ""; 
    documentCount: any = ko.observable(0);
    isAllDocuments = false;
    isSystemDocuments = false;

	constructor(public name: string) {
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
}

export = collection;