import alertArgs = require("common/alertArgs");
import alertType = require("common/alertType");
import database = require("models/database");
import filesystem = require("models/filesystem/filesystem");
import resource = require("models/resource");
import appUrl = require("common/appUrl");

/// Commands encapsulate a read or write operation to the database and support progress notifications and common AJAX related functionality.
class commandBase {

    // TODO: better place for this?
    static ravenClientVersion = '3.0.0.0';

    constructor() {
    }

    execute<T>(): JQueryPromise<T> {
        throw new Error("Execute must be overridden.");
    }

    reportInfo(title: string, details?: string) {
        this.reportProgress(alertType.info, title, details);
    }

    reportError(title: string, details?: string, httpStatusText?: string) {
        this.reportProgress(alertType.danger, title, details, httpStatusText);
        if (console && console.log && typeof console.log === "function") {
            console.log("Error during command execution", title, details, httpStatusText);
        }
    }

    reportSuccess(title: string, details?: string) {
        this.reportProgress(alertType.success, title, details);
    }

    reportWarning(title: string, details?: string, httpStatusText?: string) {
        this.reportProgress(alertType.warning, title, details, httpStatusText);
    }

    urlEncodeArgs(args: any): string {
        var propNameAndValues = [];
        for (var prop in args) {
            var value = args[prop];
            if (value instanceof Array) {
                for (var i = 0; i < value.length; i++) {
                    propNameAndValues.push(prop + "=" + encodeURIComponent(value[i]));
                }
            } else if (value !== undefined) {
                propNameAndValues.push(prop + "=" + encodeURIComponent(value));
            }
        }

        return "?" + propNameAndValues.join("&");
    }

    query<T>(relativeUrl: string, args: any, resource?: resource, resultsSelector?: (results: any) => T): JQueryPromise<T> {
        var ajax = this.ajax(relativeUrl, args, "GET", resource);
        if (resultsSelector) {
            var task = $.Deferred();
            ajax.done((results, status, xhr) => {
                var transformedResults = resultsSelector(results);
                task.resolve(transformedResults);
            });
            ajax.fail((request, status, error) => {
                task.reject(request, status, error);
                });
            return task;
        } else {
            return ajax;
        }
    }

    head<T>(relativeUrl: string, args: any, resource?: resource, resultsSelector?: (results: any) => T): JQueryPromise<T> {
        var ajax = this.ajax(relativeUrl, args, "HEAD", resource);
        if (resultsSelector) {
            var task = $.Deferred();
            ajax.done((results, status, xhr) => {
                var allHeaders = xhr.getAllResponseHeaders();
                if (allHeaders) {
                    var headersObject = {};
                    var headersArray = xhr.getAllResponseHeaders().trim().split(/\r?\n/);
                    for (var n = 0; n < headersArray.length; n++) {
                        var keyValue = headersArray[n].split(": ");
                        if (keyValue.length == 2) {
                            keyValue[1] = keyValue[1].replaceAll("\"", "");
                            headersObject[keyValue[0]] = keyValue[1];
                        }
                    }
                    var transformedResults = resultsSelector(headersObject);
                    task.resolve(transformedResults);
                }
            });
            ajax.fail((request, status, error) => {
                task.reject(request, status, error);
                });
            return task;
        } else {
            return ajax;
        }
    }

    put(relativeUrl: string, args: any, resource?: resource, options?: JQueryAjaxSettings): JQueryPromise<any> {
        return this.ajax(relativeUrl, args, "PUT", resource, options);
    }

    reset(relativeUrl: string, args: any, resource?: resource, options?: JQueryAjaxSettings): JQueryPromise<any> {
        return this.ajax(relativeUrl, args, "RESET", resource, options);
    }

    /*
     * Performs a DELETE rest call.
    */
    del(relativeUrl: string, args: any, resource?: resource, options?: JQueryAjaxSettings): JQueryPromise<any> {
        return this.ajax(relativeUrl, args, "DELETE", resource, options);
    }

    post(relativeUrl: string, args: any, resource?: resource, options?: JQueryAjaxSettings): JQueryPromise<any> {
        return this.ajax(relativeUrl, args, "POST", resource, options);
    }

    private ajax(relativeUrl: string, args: any, method: string, resource?: resource, options?: JQueryAjaxSettings): JQueryPromise<any> {
        // ContentType:
        //
        // Can't use application/json in cross-domain requests, otherwise it 
        // issues OPTIONS preflight request first, which doesn't return proper 
        // headers(e.g.Etag header, etc.)
        // 
        // So, for GETs, we issue text/plain requests, which skip the OPTIONS
        // request and goes straight for the GET request.
        var contentType = method === "GET" ?
            "text/plain; charset=utf-8" :
            "application/json; charset=utf-8";
        var defaultOptions = {
            cache: false,
            url: appUrl.forResourceQuery(resource) + relativeUrl,
            data: args,
            dataType: "json",
            contentType: contentType, 
            type: method,
            headers: undefined
        };
        
        if (options) {
            for (var prop in options) {
                defaultOptions[prop] = options[prop];
            }
        }

        return $.ajax(defaultOptions);
    }

    private reportProgress(type: alertType, title: string, details?: string, httpStatusText?: string) {
        ko.postbox.publish("Alert", new alertArgs(type, title, details));
    }
}

export = commandBase;