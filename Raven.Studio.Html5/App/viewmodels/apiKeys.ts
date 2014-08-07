import app = require("durandal/app");
import getApiKeysCommand = require("commands/getApiKeysCommand");
import apiKey = require("models/apiKey");
import viewModelBase = require("viewmodels/viewModelBase");
import getDatabasesCommand = require("commands/getDatabasesCommand");
import database = require("models/database");
import databaseAccess = require("models/databaseAccess");
import shell = require('viewmodels/shell');

class apiKeys extends viewModelBase {

    apiKeys = ko.observableArray<apiKey>().extend({ required: true });
    static globalApiKeys: KnockoutObservableArray<apiKey>;
    loadedApiKeys = ko.observableArray<apiKey>().extend({ required: true });
    allDatabases = ko.observableArray<string>();
    searchText = ko.observable<string>();
    isSaveEnabled: KnockoutComputed<boolean>;

    constructor() {
        super();

        apiKeys.globalApiKeys = this.apiKeys;
        this.searchText.throttle(200).subscribe(value => this.filterKeys(value));
        
        var databaseNames = shell.databases().filter(db => db.name != "<system>").map(db => db.name);
        this.allDatabases(databaseNames);
    }

    canActivate(args) {
        var deffered = $.Deferred();
        this.fetchApiKeys().done(() => deffered.resolve({ can: true }));

        return deffered;
    }

    activate(args) {
        super.activate(args);

        this.dirtyFlag = new ko.DirtyFlag([this.apiKeys]);
        this.isSaveEnabled = ko.computed(() => this.dirtyFlag().isDirty());
    }

    private fetchApiKeys(): JQueryPromise<any> {
        return new getApiKeysCommand()
            .execute()
            .done((results: apiKey[]) => {
                this.apiKeys(results);
                this.saveLoadedApiKeys(results);
                apiKeys.globalApiKeys(results);
                this.apiKeys().forEach((key: apiKey) => {
                    this.subscribeToObservable(key);
                });
        });
    }

    private saveLoadedApiKeys(apiKeys: apiKey[]) {
        // clones the apiKeys object
        this.loadedApiKeys = ko.mapping.fromJS(ko.mapping.toJS(apiKeys));
    }

    private subscribeToObservable(key: apiKey) {
        key.name.subscribe((previousApiKeyName) => {
            var existingApiKeysExceptCurrent = this.apiKeys().filter((k: apiKey) => k !== key && k.name() == previousApiKeyName);
            if (existingApiKeysExceptCurrent.length == 1) {
                existingApiKeysExceptCurrent[0].nameCustomValidity('');
            }
        }, this, "beforeChange");
        key.name.subscribe((newApiKeyName) => {
            var errorMessage: string = '';
            var isApiKeyNameValid = newApiKeyName.indexOf("\\") == -1;
            var existingApiKeys = this.apiKeys().filter((k: apiKey) => k !== key && k.name() == newApiKeyName);

            if (isApiKeyNameValid == false) {
                errorMessage = "API Key name cannot contain '\\'";
            } else if (existingApiKeys.length > 0) {
                errorMessage = "API key name already exists!";
            }

            key.nameCustomValidity(errorMessage);
        });
    }

    createNewApiKey() {
        var newApiKey = apiKey.empty();
        this.subscribeToObservable(newApiKey);
        this.apiKeys.unshift(newApiKey);
    }

    removeApiKey(key: apiKey) {
        this.apiKeys.remove(key);
    }

    saveChanges() {
        this.apiKeys().forEach((key: apiKey) => key.setIdFromName());

        var apiKeysNamesArray: Array<string> = this.apiKeys().map((key: apiKey) => key.name());
        var deletedApiKeys = this.loadedApiKeys().filter((key: apiKey) => apiKeysNamesArray.contains(key.name()) == false);
        deletedApiKeys.forEach((key: apiKey) => key.setIdFromName());

        require(["commands/saveApiKeysCommand"], saveApiKeysCommand => {
            new saveApiKeysCommand(this.apiKeys(), deletedApiKeys)
                .execute()
                .done((result: bulkDocumentDto[]) => {
                    this.updateKeys(result);
                    this.saveLoadedApiKeys(this.apiKeys());
                    this.dirtyFlag().reset();
                });
        });
    }

    updateKeys(serverKeys: bulkDocumentDto[]) {
        this.apiKeys().forEach(key => {
            var serverKey = serverKeys.first(k => k.Key === key.getId());
            if (serverKey) {
                key.__metadata.etag = serverKey.Etag;
                key.__metadata.lastModified = serverKey.Metadata['Last-Modified'];
            }
        });
    }

    filterKeys(filter: string) {
        var filterLower = filter.toLowerCase().trim();
        this.apiKeys().forEach(k => {
            var isEmpty = filterLower.length === 0;
            var isMatch = k.name() != null && k.name().toLowerCase().indexOf(filterLower) !== -1;
            k.visible(isEmpty || isMatch);
        });
    }
}

export = apiKeys;
