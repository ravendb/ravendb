define(["require", "exports", "common/raven", "common/alertArgs", "common/alertType", "models/database"], function(require, exports, __raven__, __alertArgs__, __alertType__, __database__) {
    var raven = __raven__;
    var alertArgs = __alertArgs__;
    var alertType = __alertType__;
    var database = __database__;

    /// Commands encapsulate a read or write operation to the database and support progress notifications and common AJAX related functionality.
    var commandBase = (function () {
        //private baseUrl = ""; // This should be used when serving HTML5 Studio from the server app.
        function commandBase() {
            this.baseUrl = "http://localhost:8080";
            this.ravenDb = new raven();
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
                    if (results && results.length >= 0) {
                        task.resolve(results.map(function (r) {
                            return resultsSelector(r);
                        }));
                    } else {
                        // Results isn't an array. Apply the selector directly on the result object.
                        task.resolve(resultsSelector(results));
                    }
                });
                ajax.fail(function (request, status, error) {
                    return task.reject(request, status, error);
                });
                return task;
            } else {
                return ajax;
            }
        };

        commandBase.prototype.reportProgress = function (type, title, details) {
            ko.postbox.publish("Alert", new alertArgs(type, title, details));
        };

        commandBase.prototype.ajax = function (relativeUrl, args, method, database, customHeaders) {
            var options = {
                cache: false,
                url: this.getDatabaseUrl(database) + relativeUrl,
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

        commandBase.prototype.getDatabaseUrl = function (database) {
            if (database && !database.isSystem) {
                return this.baseUrl + "/databases/" + database.name;
            }

            return this.baseUrl;
        };
        return commandBase;
    })();

    
    return commandBase;
});
//# sourceMappingURL=commandBase.js.map
