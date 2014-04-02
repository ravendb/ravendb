import pagedList = require("common/pagedList");
import pagedResultSet = require("common/pagedResultSet");
import getConfigurationByKeyCommand = require("commands/filesystem/getConfigurationByKeyCommand");
import filesystem = require("models/filesystem/filesystem");

class configurationKey {

    private valuesList: pagedList;

    constructor(public fs: filesystem, public key) {
    }

    // Notifies consumers that this configuration key should be the selected one.
    // Called from the UI when a user clicks a configuration key the configuration page.
    activate() {
        ko.postbox.publish("ActivateConfigurationKey", this);
    }

    getValues(): pagedList {
        if (!this.valuesList) {
            this.valuesList = this.createPagedList();
        }

        return this.valuesList;
    }

    fetchValues(skip: number, take: number): JQueryPromise<pagedResultSet> {
        return new getConfigurationByKeyCommand(this.fs, this.key).execute();
    }

    private createPagedList(): pagedList {
        var fetcher = (skip: number, take: number) => this.fetchValues(skip, take);
        var list = new pagedList(fetcher);
        list.collectionName = this.key;
        return list;
    }

}

export = configurationKey;
