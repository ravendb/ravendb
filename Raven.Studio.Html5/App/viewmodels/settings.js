define(["require", "exports", "common/raven", "models/apiKey"], function(require, exports, __raven__, __apiKey__) {
    var raven = __raven__;
    var apiKey = __apiKey__;

    var settings = (function () {
        function settings() {
            this.activeDatabase = raven.activeDatabase;
            this.isShowingApiKeys = ko.observable(true);
            this.apiKeys = ko.observableArray();
            // Some temporary dummy data as placeholder until we're ready to fetch from server.
            var dummyApiKeyDto = {
                name: "dummy API key",
                enabled: true,
                secret: "6JIAgXI6tzP",
                fullApiKey: "dummyfoo/6JIAgXI6tzP",
                connectionString: "Url = http://localhost:8080/; ApiKey = dummyfoo/6JIAgXI6tzP; Database = ",
                directLink: "http://localhost:8080/raven/studio.html#/home?api-key=dummyfoo/6JIAgXI6tzP",
                databases: [
                    { name: "dummy", admin: true, readOnly: false },
                    { name: "foobar", admin: false, readOnly: true }
                ]
            };
            this.apiKeys.push(new apiKey(dummyApiKeyDto, this.activeDatabase().name));
        }
        settings.prototype.saveChanges = function () {
        };

        settings.prototype.showApiKeys = function () {
            this.isShowingApiKeys(true);
        };

        settings.prototype.showWindowsAuth = function () {
            this.isShowingApiKeys(false);
        };

        settings.prototype.createNewApiKey = function () {
            this.apiKeys.unshift(apiKey.empty(this.activeDatabase().name));
        };

        settings.prototype.removeApiKey = function (key) {
            this.apiKeys.remove(key);
        };
        return settings;
    })();

    
    return settings;
});
//# sourceMappingURL=settings.js.map
