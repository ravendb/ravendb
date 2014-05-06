import pagedList = require("common/pagedList");
import pagedResultSet = require("common/pagedResultSet");
import getConfigurationByKeyCommand = require("commands/filesystem/getConfigurationByKeyCommand");
import filesystem = require("models/filesystem/filesystem");

class configurationKey {

    constructor(public fs: filesystem, public key: string) {
    }

    // Notifies consumers that this configuration key should be the selected one.
    // Called from the UI when a user clicks a configuration key the configuration page.
    activate() {
        ko.postbox.publish("ActivateConfigurationKey", this);
    }

    getValues(): JQueryPromise<string> {
        return new getConfigurationByKeyCommand(this.fs, this.key).execute();
    }

}

export = configurationKey;
