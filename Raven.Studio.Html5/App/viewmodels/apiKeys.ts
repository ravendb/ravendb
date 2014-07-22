import app = require("durandal/app");
import getApiKeysCommand = require("commands/getApiKeysCommand");
import apiKey = require("models/apiKey");
import viewModelBase = require("viewmodels/viewModelBase");
import getDatabasesCommand = require("commands/getDatabasesCommand");
import database = require("models/database");
import saveApiKeysCommand = require("commands/saveApiKeysCommand");
import databaseAccess = require("models/databaseAccess");
import shell = require('viewmodels/shell');

class apiKeys extends viewModelBase {

    apiKeys = ko.observableArray<apiKey>().extend({ required: true });
    allDatabases = ko.observableArray<string>();
    searchText = ko.observable<string>();
    hasFetchedApiKeys = ko.observable(false);
    isSaveEnabled: KnockoutComputed<boolean>;

    constructor() {
        super();

        this.searchText.throttle(200).subscribe(value => this.filterKeys(value));

        var databaseNames = shell.databases().filter(db => db.name != "<system>").map(db => db.name);
        this.allDatabases(databaseNames);
    }

    canActivate(args) {
        super.canActivate(args);

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
            .done(results => {
                this.apiKeys(results);
                this.hasFetchedApiKeys(true);
            });
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
            .done((result: bulkDocumentDto[]) => {
                this.updateKeys(result);
                this.dirtyFlag().reset();
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
