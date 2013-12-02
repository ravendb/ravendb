import raven = require("common/raven");
import alertArgs = require("common/alertArgs");
import alertType = require("common/alertType");
import database = require("models/database");

/// Commands encapsulate a read or write operation to the database and support progress notifications and common AJAX related functionality.
class commandBase {
    ravenDb: raven;
    private baseUrl = "http://localhost:8080"; // For debugging purposes, uncomment this line to point Raven at an already-running Raven server. Requires the Raven server to have it's config set to <add key="Raven/AccessControlAllowOrigin" value="*" />
    //private baseUrl = ""; // This should be used when serving HTML5 Studio from the server app.

    constructor() {
        this.ravenDb = new raven();
    }

    execute<T>(): JQueryPromise<T> {
        throw new Error("Execute must be overridden.");
    }

    reportInfo(title: string, details?: string) {
        this.reportProgress(alertType.info, title, details);
    }

    reportError(title: string, details?: string) {
        this.reportProgress(alertType.danger, title, details);
    }

    reportSuccess(title: string, details?: string) {
        this.reportProgress(alertType.success, title, details);
    }

    reportWarning(title: string, details?: string) {
        this.reportProgress(alertType.warning, title, details);
    }

    query<T>(relativeUrl: string, args: any, database?: database, resultsSelector?: (results: any) => T): JQueryPromise<T> {
        var ajax = this.ajax(relativeUrl, args, "GET", database);
        if (resultsSelector) {
            var task = $.Deferred();
            ajax.done((results) => {
                // If results is an array, apply the resultsSelector to each element.
                if (results && results.length >= 0) {
                    task.resolve(results.map(r => resultsSelector(r)));
                } else {
                    // Results isn't an array. Apply the selector directly on the result object.
                    task.resolve(resultsSelector(results));
                }
            });
            ajax.fail((request, status, error) => task.reject(request, status, error));
            return task;
        } else {
            return ajax;
        }
    }

    private reportProgress(type: alertType, title: string, details?: string) {
        ko.postbox.publish("Alert", new alertArgs(type, title, details));
    }

    private ajax(relativeUrl: string, args: any, method: string, database?: database, customHeaders?: any): JQueryPromise<any> {

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
    }

    private getDatabaseUrl(database: database) {
        if (database && !database.isSystem) {
            return this.baseUrl + "/databases/" + database.name;
        }

        return this.baseUrl;
    }
}

export = commandBase;