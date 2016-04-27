import getApiKeysCommand = require("commands/auth/getApiKeysCommand");
import saveApiKeysCommand = require("commands/auth/saveApiKeysCommand");
import apiKey = require("models/auth/apiKey");
import viewModelBase = require("viewmodels/viewModelBase");
import settingsAccessAuthorizer = require("common/settingsAccessAuthorizer");

class apiKeys extends viewModelBase {
    apiKeys = ko.observableArray<apiKey>();
    loadedApiKeys = ko.observableArray<apiKey>();
    searchText = ko.observable<string>();
    isSaveEnabled: KnockoutComputed<boolean>;
    noResults = ko.observable<boolean>(false);

    settingsAccess = new settingsAccessAuthorizer();

    constructor() {
        super();
        this.searchText.throttle(200).subscribe(value => this.filterKeys(value));
    }

    canActivate(args) {
        var deferred = $.Deferred();

        if (this.settingsAccess.isForbidden()) {
            deferred.resolve({ can: true });
        } else {
            this.fetchApiKeys().done(() => deferred.resolve({ can: true }));
        }
        
        return deferred;
    }

    activate(args) {
        super.activate(args);
        this.updateHelpLink('9CGJ4Y');
        this.dirtyFlag = new ko.DirtyFlag([this.apiKeys]);
        this.isSaveEnabled = ko.computed(() => !this.settingsAccess.isReadOnly() && this.dirtyFlag().isDirty());
    }

    compositionComplete() {
        super.compositionComplete();
        if (this.settingsAccess.isReadOnly()) {
            $('#manageApiKeys form input:not(#apiKeysSearchInput)').attr('readonly', 'readonly');
            $('#manageApiKeys button').attr('disabled', 'true');
        }
        $("#manageApiKeys form").on("keypress", 'input[name="databaseName"]', (e) => e.which !== 13);
    }

    private fetchApiKeys(): JQueryPromise<any> {
        return new getApiKeysCommand()
            .execute()
            .done((results: apiKey[]) => {
                this.apiKeys(results);
                this.saveLoadedApiKeys(results);
                this.apiKeys().forEach((key: apiKey) => {
                    this.subscribeToObservableKeyName(key);
                });
        });
    }

    private saveLoadedApiKeys(apiKeys: apiKey[]) {
        // clones the apiKeys object
        this.loadedApiKeys = ko.mapping.fromJS(ko.mapping.toJS(apiKeys));
    }

    private subscribeToObservableKeyName(key: apiKey) {
        key.name.subscribe((previousApiKeyName) => {
            var existingApiKeysExceptCurrent = this.apiKeys().filter((k: apiKey) => k !== key && k.name() === previousApiKeyName);
            if (existingApiKeysExceptCurrent.length === 1) {
                existingApiKeysExceptCurrent[0].nameCustomValidity('');
            }
        }, this, "beforeChange");
        key.name.subscribe((newApiKeyName) => {
            var errorMessage: string = '';
            var isApiKeyNameValid = newApiKeyName.indexOf("\\") === -1;
            var existingApiKeys = this.apiKeys().filter((k: apiKey) => k !== key && k.name() === newApiKeyName);

            if (!isApiKeyNameValid) {
                errorMessage = "API Key name cannot contain '\\'";
            } else if (existingApiKeys.length > 0) {
                errorMessage = "API key name already exists!";
            }

            key.nameCustomValidity(errorMessage);
        });
    }

    createNewApiKey() {
        var newApiKey = apiKey.empty();
        this.subscribeToObservableKeyName(newApiKey);
        newApiKey.generateSecret();
        this.apiKeys.unshift(newApiKey);
    }

    removeApiKey(key: apiKey) {
        this.apiKeys.remove(key);
    }

    saveChanges() {
        this.apiKeys().forEach((key: apiKey) => key.setIdFromName());

        var apiKeysNamesArray: Array<string> = this.apiKeys().map((key: apiKey) => key.name());
        var deletedApiKeys = this.loadedApiKeys().filter((key: apiKey) => !apiKeysNamesArray.contains(key.name()));
        deletedApiKeys.forEach((key: apiKey) => key.setIdFromName());

        new saveApiKeysCommand(this.apiKeys(), deletedApiKeys)
            .execute()
            .done((result: bulkDocumentDto[]) => {
                this.updateKeys(result);
                this.saveLoadedApiKeys(this.apiKeys());
                this.dirtyFlag().reset();
            });
    }

    updateKeys(serverKeys: bulkDocumentDto[]) {
        this.apiKeys().forEach(key => {
            var serverKey = serverKeys.first(k => k.Key === key.getId());
            if (serverKey) {
                key.__metadata.etag = serverKey.Etag;
                key.__metadata.lastModified = serverKey.Metadata["Last-Modified"];
            }
        });
    }

    filterKeys(filter: string) {
        var filterLower = filter.toLowerCase().trim();
        var matches = 0;
        this.apiKeys().forEach(k => {
            var isEmpty = filterLower.length === 0;
            var isMatch = k.name() != null && k.name().toLowerCase().indexOf(filterLower) !== -1;
            var visible = isEmpty || isMatch;
            k.visible(visible);
            if (visible) {
                matches++;
            }
        });

        this.noResults(matches === 0);
    }
}

export = apiKeys;
