import getApiKeysCommand = require("commands/getApiKeysCommand");
import saveApiKeyCommand = require("commands/saveApiKeysCommand");
import apiKey = require("models/apiKey");
import viewModelBase = require("viewmodels/viewModelBase");

class apiKeys extends viewModelBase{

    apiKeys = ko.observableArray<apiKey>();
    filter = ko.observable<String>();
    filteredApiKeys = ko.computed(()=> {
        

        if (!this.filter()) {
            return this.apiKeys();
        } else {
            var filter = this.filter().toString();
            filter = this.filter().toLowerCase();
            return ko.utils.arrayFilter(this.apiKeys(), item=> {
                return item.name().toLowerCase().indexOf(filter) !== -1;
            });
        }
    });
    

    constructor() {
        super();
    }

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
        var db = this.activeDatabase();
        new saveApiKeyCommand(this.apiKeys(), db).execute();
        

    }
    
}

export = apiKeys;  