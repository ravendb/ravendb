define(["require", "exports", "models/apiKeyDatabase", "common/appUrl"], function(require, exports, __apiKeyDatabase__, __appUrl__) {
    var apiKeyDatabase = __apiKeyDatabase__;
    var appUrl = __appUrl__;

    var apiKey = (function () {
        function apiKey(dto) {
            var _this = this;
            this.name = ko.observable();
            this.secret = ko.observable();
            this.enabled = ko.observable();
            this.databases = ko.observableArray();
            this.name(dto.Name);
            this.secret(dto.Secret);
            this.enabled(dto.Enabled);
            this.databases(dto.Databases.map(function (d) {
                return new apiKeyDatabase(d);
            }));

            this.fullApiKey = ko.computed(function () {
                if (!_this.name() || !_this.secret()) {
                    return "Requires name and secret";
                }

                return _this.name() + "/" + _this.secret();
            });

            this.connectionString = ko.computed(function () {
                if (!_this.fullApiKey()) {
                    return "Requires name and secret";
                }

                return "Url = " + appUrl.forServer() + "; ApiKey = " + _this.fullApiKey() + "; Database = ";
            });

            this.directLink = ko.computed(function () {
                if (!_this.fullApiKey()) {
                    return "Requires name and secret";
                }

                return appUrl.forServer() + "/raven/studio.html#/home?api-key=" + _this.fullApiKey();
            });
        }
        apiKey.empty = function () {
            return new apiKey({
                Databases: [],
                Enabled: false,
                Name: "[new api key]",
                Secret: ""
            });
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
