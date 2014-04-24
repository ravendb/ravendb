import pagedList = require("common/pagedList");
import pagedResultSet = require("common/pagedResultSet");
import filesystem = require("models/filesystem/filesystem");

class filesystemCollection{

    itemsCount: any = ko.observable(0);
    isAllItems = false;
    
    private itemsList: pagedList;

    constructor(public name: string, public owner: filesystem ) {
    }

    // Notifies consumers that this collection should be the selected one.
    // Called from the UI when a user clicks a collection the documents page.
    activate() {
        ko.postbox.publish("ActivateCollection", this);
    }

    getItems(): pagedList {
        if (!this.itemsList) {
            this.itemsList = this.createPagedList();
        }

        return this.itemsList;
    }

    fetchItems(skip: number, take: number): JQueryPromise<pagedResultSet> {
        throw new Error('This method is abstract');
    }

    private createPagedList(): pagedList {
        var fetcher = (skip: number, take: number) => this.fetchItems(skip, take);
        var list = new pagedList(fetcher);
        list.collectionName = this.name;
        return list;
    }

}

export = filesystemCollection;