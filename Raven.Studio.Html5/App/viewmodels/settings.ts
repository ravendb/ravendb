import raven = require("common/raven");
import apiKey = require("models/apiKey");

class settings {

    activeDatabase = raven.activeDatabase;
    isShowingApiKeys = ko.observable(true);
    apiKeys = ko.observableArray<apiKey>();

    constructor() {

        // Some temporary dummy data as placeholder until we're ready to fetch from server.
        var dummyApiKeyDto = {
            name: "dummyAPIkey",
            enabled: true,
            secret: "6JIAgXI6tzP",
            fullApiKey: "dummyAPIkey/6JIAgXI6tzP",
            connectionString: "Url = http://localhost:8080/; ApiKey = dummyAPIkey/6JIAgXI6tzP; Database = ",
            directLink: "http://localhost:8080/raven/studio.html#/home?api-key=dummyAPIkey/6JIAgXI6tzP",
            databases: [
                { name: "dummy", admin: true, readOnly: false },
                { name: "foobar", admin: false, readOnly: true }
            ]
        };
        this.apiKeys.push(new apiKey(dummyApiKeyDto, this.activeDatabase().name));
    }

    saveChanges() {
    }

    showApiKeys() {
        this.isShowingApiKeys(true);
    }

    showWindowsAuth() {
        this.isShowingApiKeys(false);
    }

    createNewApiKey() {
        this.apiKeys.unshift(apiKey.empty(this.activeDatabase().name));
    }

    removeApiKey(key: apiKey) {
        this.apiKeys.remove(key);
    }
}

export = settings;