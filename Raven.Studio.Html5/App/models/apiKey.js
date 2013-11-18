define(["require", "exports", "models/apiKeyDatabase", "common/appUrl"], function(require, exports, __apiKeyDatabase__, __appUrl__) {
    var apiKeyDatabase = __apiKeyDatabase__;
    var appUrl = __appUrl__;

    var apiKey = (function () {
        function apiKey(dto, databaseName) {
            var _this = this;
            this.databaseName = databaseName;
            this.name = ko.observable();
            this.secret = ko.observable();
            this.fullApiKey = ko.observable();
            this.connectionString = ko.observable();
            this.directLink = ko.observable();
            this.enabled = ko.observable();
            this.databases = ko.observableArray();
            this.name(dto.name);
            this.secret(dto.secret);
            this.connectionString(dto.connectionString);
            this.directLink(dto.directLink);
            this.enabled(dto.enabled);
            this.databases(dto.databases.map(function (d) {
                return new apiKeyDatabase(d);
            }));
            this.fullApiKey(dto.fullApiKey);

            this.name.subscribe(function (newName) {
                return _this.onNameOrSecretChanged(newName, _this.secret());
            });
            this.secret.subscribe(function (newSecret) {
                return _this.onNameOrSecretChanged(_this.name(), newSecret);
            });
        }
        apiKey.empty = function (databaseName) {
            return new apiKey({
                connectionString: "",
                databases: [],
                directLink: "",
                enabled: false,
                fullApiKey: "",
                name: "[new api key]",
                secret: ""
            }, databaseName);
        };

        apiKey.prototype.enable = function () {
            this.enabled(true);
        };

        apiKey.prototype.disable = function () {
            this.enabled(false);
        };

        apiKey.prototype.generateSecret = function () {
            // The old Silverlight Studio would create a new GUID, strip out the
            // dashes, and convert to base62.
            //
            // For the time being (is there a better way?), we're just creating a
            // random string of alpha numeric characters.
            var minimumLength = 10;
            var maxLength = 32;
            var randomLength = Math.max(minimumLength, Math.random() * maxLength);
            var randomSecret = apiKey.randomString(randomLength, '0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ');
            this.secret(randomSecret);
        };

        apiKey.prototype.onNameOrSecretChanged = function (name, secret) {
            if (!name || !secret) {
                var errorText = "Requires name and secret";
                this.fullApiKey(errorText);
                this.connectionString(errorText);
                this.directLink(errorText);
            } else {
                var serverUrl = appUrl.forServer();
                this.fullApiKey(name + "/" + secret);
                this.connectionString("Url = " + serverUrl + "; ApiKey = " + this.fullApiKey() + "; Database = " + this.databaseName);
                this.directLink(serverUrl + "/raven/studio.html#/home?api-key=" + this.fullApiKey());
            }
        };

        apiKey.randomString = function (length, chars) {
            var result = '';
            for (var i = length; i > 0; --i)
                result += chars[Math.round(Math.random() * (chars.length - 1))];
            return result;
        };
        return apiKey;
    })();

    
    return apiKey;
});
//# sourceMappingURL=apiKey.js.map
