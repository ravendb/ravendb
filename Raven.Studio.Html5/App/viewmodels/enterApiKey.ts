import dialog = require("plugins/dialog");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import oauthContext = require("common/oauthContext");
import apiKeyLocalStorage = require("common/apiKeyLocalStorage");

class enterApiKey extends dialogViewModelBase {

    apiKey = ko.observable("");
    saveInLocalStorage = ko.observable<boolean>(false);

    cancel() {
        dialog.close(this);
    }

    ok() {
        oauthContext.apiKey(this.apiKey());
        if (this.saveInLocalStorage()) {
            apiKeyLocalStorage.setValue(this.apiKey());
        } else {
            apiKeyLocalStorage.clean();
        }
        dialog.close(this);
    }

}

export = enterApiKey;
