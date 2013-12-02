import getApiKeysCommand = require("commands/getApiKeysCommand");
import apiKey = require("models/apiKey");

class apiKeys {

    apiKeys = ko.observableArray<apiKey>();

    activate() {
        new getApiKeysCommand()
            .execute()
            .done(results => this.apiKeys(results));
    }

    createNewApiKey() {
        this.apiKeys.unshift(apiKey.empty());
    }

    removeApiKey(key: apiKey) {
        this.apiKeys.remove(key);
    }

    saveChanges() {
    }
}

export = apiKeys;