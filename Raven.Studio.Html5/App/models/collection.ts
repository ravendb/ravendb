import raven = require("common/raven");
import pagedList = require("common/pagedList");

class collection {

    colorClass = ""; 
	documentCount = ko.observable(0);

	constructor(public name: string, public isAllCollections?: boolean) {
	}

	// Notifies consumers that this collection should be the selected one.
	// Called from the UI when a user clicks a collection the documents page.
	activate() {
		ko.postbox.publish("ActivateCollection", this);
    }
}

export = collection;