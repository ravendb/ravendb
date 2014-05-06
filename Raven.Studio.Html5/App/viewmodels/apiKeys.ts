import app = require("durandal/app");
import getApiKeysCommand = require("commands/getApiKeysCommand");
import apiKey = require("models/apiKey");
import viewModelBase = require("viewmodels/viewModelBase");
import getDatabasesCommand = require("commands/getDatabasesCommand");
import database = require("models/database");
import saveApiKeysCommand = require("commands/saveApiKeysCommand");
import databaseAccess = require("models/databaseAccess");

class apiKeys extends viewModelBase {

    apiKeys = ko.observableArray<apiKey>();
    allDatabases = ko.observableArray<string>();
    areAllApiKeysValid: KnockoutComputed<boolean>;
    searchText = ko.observable<string>();
    hasFetchedApiKeys = ko.observable(false);

    constructor() {
        super();

        this.areAllApiKeysValid = ko.computed(() => this.apiKeys().every(k => k.isValid()));
        this.searchText.throttle(200).subscribe(value => this.filterKeys(value));
    }

    activate(args: any) {
        super.activate(args);

        this.fetchApiKeys();
        this.fetchDatabases();
    }

    fetchApiKeys() {
        new getApiKeysCommand()
            .execute()
            .done(results => {
                this.apiKeys(results);
                this.hasFetchedApiKeys(true);
            });
    }

    fetchDatabases() {
        new getDatabasesCommand()
            .execute()
            .done((results: database[]) => this.allDatabases(results.map(d => d.name)));
    }

    createNewApiKey() {
        this.apiKeys.unshift(apiKey.empty());
    }

    removeApiKey(key: apiKey) {
        this.apiKeys.remove(key);
    }

    saveChanges() {
        this.apiKeys().forEach(k => k.setIdFromName());
        new saveApiKeysCommand(this.apiKeys(), this.activeDatabase())
            .execute()
            .done((result: bulkDocumentDto[]) => this.updateKeys(result));
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
