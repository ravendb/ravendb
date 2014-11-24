import dialog = require("plugins/dialog");
import createDatabaseCommand = require("commands/createDatabaseCommand");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import oauthContext = require("common/oauthContext");

class enterApiKey extends dialogViewModelBase {

    apiKey = ko.observable("");

    cancel() {
        dialog.close(this);
    }

    ok() {
        oauthContext.apiKey(this.apiKey());
        dialog.close(this);
    }

}

export = enterApiKey;
