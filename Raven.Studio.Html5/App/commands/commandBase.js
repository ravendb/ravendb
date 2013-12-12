define(["require", "exports", "common/alertArgs", "common/alertType", "models/database", "common/appUrl"], function(require, exports, __alertArgs__, __alertType__, __database__, __appUrl__) {
    
    var alertArgs = __alertArgs__;
    var alertType = __alertType__;
    var database = __database__;
    var appUrl = __appUrl__;

    /// Commands encapsulate a read or write operation to the database and support progress notifications and common AJAX related functionality.
    var commandBase = (function () {
        function commandBase() {
            this.baseUrl = "http://localhost:8080";
        }
        commandBase.prototype.execute = function () {
            throw new Error("Execute must be overridden.");
        };

        commandBase.prototype.reportInfo = function (title, details) {
            this.reportProgress(alertType.info, title, details);
        };

        commandBase.prototype.reportError = function (title, details) {
            this.reportProgress(alertType.danger, title, details);
        };

        commandBase.prototype.reportSuccess = function (title, details) {
            this.reportProgress(alertType.success, title, details);
        };

        commandBase.prototype.reportWarning = function (title, details) {
            this.reportProgress(alertType.warning, title, details);
        };

        commandBase.prototype.query = function (relativeUrl, args, database, resultsSelector) {
            var ajax = this.ajax(relativeUrl, args, "GET", database);
            if (resultsSelector) {
                var task = $.Deferred();
                ajax.done(function (results) {
                    task.resolve(resultsSelector(results));
                });
                ajax.fail(function (request, status, error) {
                    return task.reject(request, status, error);
                });
                return task;
            } else {
                return ajax;
            }
        };

        commandBase.prototype.put = function (relativeUrl, args, database, customHeaders) {
            return this.ajax(relativeUrl, args, "PUT", database, customHeaders);
        };

        /*
        * Performs a DELETE rest call.
        */
        commandBase.prototype.del = function (relativeUrl, args, database, customHeaders) {
            return this.ajax(relativeUrl, args, "DELETE", database, customHeaders);
        };

        commandBase.prototype.post = function (relativeUrl, args, database, customHeaders) {
            return this.ajax(relativeUrl, args, "POST", database, customHeaders);
        };

        commandBase.prototype.ajax = function (relativeUrl, args, method, database, customHeaders) {
            var options = {
                cache: false,
                url: appUrl.forDatabaseQuery(database) + relativeUrl,
                data: args,
                contentType: "application/json; charset=utf-8",
                type: method,
                headers: undefined
            };

            if (customHeaders) {
                options.headers = customHeaders;
            }

            return $.ajax(options);
        };

        commandBase.prototype.reportProgress = function (type, title, details) {
            ko.postbox.publish("Alert", new alertArgs(type, title, details));
        };
        commandBase.ravenClientVersion = '3.0.0.0';
        return commandBase;
    })();

    
    return commandBase;
});
//# sourceMappingURL=commandBase.js.map
