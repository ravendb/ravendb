define(["require", "exports"], function(require, exports) {
    var counterStorageReplicationDestination = (function () {
        function counterStorageReplicationDestination(dto) {
            var _this = this;
            this.disabled = ko.observable().extend({ required: true });
            this.serverUrl = ko.observable().extend({ required: true });
            this.counterStorageName = ko.observable().extend({ required: true });
            this.username = ko.observable().extend({ required: true });
            this.password = ko.observable().extend({ required: true });
            this.domain = ko.observable().extend({ required: true });
            this.apiKey = ko.observable().extend({ required: true });
            this.name = ko.computed(function () {
                if (_this.serverUrl() && _this.counterStorageName()) {
                    return _this.counterStorageName() + " on " + _this.serverUrl();
                } else if (_this.serverUrl()) {
                    return _this.serverUrl();
                } else if (_this.counterStorageName()) {
                    return _this.counterStorageName();
                }

                return "[empty]";
            });
            this.isValid = ko.computed(function () {
                return _this.serverUrl() != null && _this.serverUrl().length > 0;
            });
            // data members for the ui
            this.isUserCredentials = ko.observable(false);
            this.isApiKeyCredentials = ko.observable(false);
            this.credentialsType = ko.computed(function () {
                if (_this.isUserCredentials()) {
                    return "user";
                } else if (_this.isApiKeyCredentials()) {
                    return "api-key";
                } else {
                    return "none";
                }
            });
            this.disabled(dto.Disabled);
            this.serverUrl(dto.ServerUrl);
            this.counterStorageName(dto.CounterStorageName);
            this.username(dto.Username);
            this.password(dto.Password);
            this.domain(dto.Domain);
            this.apiKey(dto.ApiKey);

            if (this.username()) {
                this.isUserCredentials(true);
            } else if (this.apiKey()) {
                this.isApiKeyCredentials(true);
            }
        }
        counterStorageReplicationDestination.prototype.toggleUserCredentials = function () {
            this.isUserCredentials.toggle();
            if (this.isUserCredentials()) {
                this.isApiKeyCredentials(false);
            }
        };

        counterStorageReplicationDestination.prototype.toggleApiKeyCredentials = function () {
            this.isApiKeyCredentials.toggle();
            if (this.isApiKeyCredentials()) {
                this.isUserCredentials(false);
            }
        };

        counterStorageReplicationDestination.empty = function (counterStorageName) {
            return new counterStorageReplicationDestination({
                Disabled: false,
                ServerUrl: null,
                CounterStorageName: counterStorageName,
                Username: null,
                Password: null,
                Domain: null,
                ApiKey: null
            });
        };

        counterStorageReplicationDestination.prototype.enable = function () {
            this.disabled(false);
        };

        counterStorageReplicationDestination.prototype.disable = function () {
            this.disabled(true);
        };

        counterStorageReplicationDestination.prototype.toDto = function () {
            return {
                Disabled: this.disabled(),
                ServerUrl: this.prepareUrl(),
                CounterStorageName: this.counterStorageName(),
                Username: this.username(),
                Password: this.password(),
                Domain: this.domain(),
                ApiKey: this.apiKey()
            };
        };

        counterStorageReplicationDestination.prototype.prepareUrl = function () {
            var url = this.serverUrl();
            if (url && url.charAt(url.length - 1) === "/") {
                url = url.substring(0, url.length - 1);
            }
            return url;
        };
        return counterStorageReplicationDestination;
    })();

    
    return counterStorageReplicationDestination;
});
//# sourceMappingURL=counterStorageReplicationDestination.js.map
