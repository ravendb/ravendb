define(["require", "exports"], function(require, exports) {
    var counterStorageReplicationDestination = (function () {
        function counterStorageReplicationDestination() {
            var _this = this;
            this.serverUrl = ko.observable().extend({ required: true });
            this.username = ko.observable().extend({ required: true });
            this.password = ko.observable().extend({ required: true });
            this.domain = ko.observable().extend({ required: true });
            this.apiKey = ko.observable().extend({ required: true });
            this.counterStorage = ko.observable().extend({ required: true });
            this.disabled = ko.observable().extend({ required: true });
            this.name = ko.computed(function () {
                if (_this.serverUrl() && _this.counterStorage()) {
                    return _this.counterStorage() + " on " + _this.serverUrl();
                } else if (_this.serverUrl()) {
                    return _this.serverUrl();
                } else if (_this.counterStorage()) {
                    return _this.counterStorage();
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
        }
        return counterStorageReplicationDestination;
    })();

    
    return counterStorageReplicationDestination;
});
//# sourceMappingURL=counterStorageReplicationDestination.js.map
