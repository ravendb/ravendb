define(["require", "exports", "commands/getApiKeysCommand", "models/apiKey"], function(require, exports, getApiKeysCommand, apiKey) {
    var apiKeys = (function () {
        function apiKeys() {
            this.apiKeys = ko.observableArray();
        }
        apiKeys.prototype.activate = function () {
            var _this = this;
            new getApiKeysCommand().execute().done(function (results) {
                return _this.apiKeys(results);
            });
        };

        apiKeys.prototype.createNewApiKey = function () {
            this.apiKeys.unshift(apiKey.empty());
        };

        apiKeys.prototype.removeApiKey = function (key) {
            this.apiKeys.remove(key);
        };

        apiKeys.prototype.saveChanges = function () {
        };
        return apiKeys;
    })();

    
    return apiKeys;
});
//# sourceMappingURL=apiKeys.js.map
