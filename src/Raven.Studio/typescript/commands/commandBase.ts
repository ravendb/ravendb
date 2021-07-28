/// <reference path="../../typings/tsd.d.ts" />

import messagePublisher = require("common/messagePublisher");
import database = require("models/resources/database");
import appUrl = require("common/appUrl");
import protractedCommandsDetector = require("common/notifications/protractedCommandsDetector");

/// Commands encapsulate a read or write operation to the database and support progress notifications and common AJAX related functionality.
class commandBase {

    static ravenClientVersion = '5.2.0.0';

    static ravenStudioVersionHeader = 'Raven-Studio-Version';

    execute(): JQueryPromise<any> {
        throw new Error("Execute must be overridden.");
    }

    urlEncodeArgs(args: any): string {
        return appUrl.urlEncodeArgs(args);
    }

    getTimeToAlert(longWait: boolean): number {
        return longWait ? 60000 : 9000;
    }

    query<T>(relativeUrl: string, args: any, db?: database, resultsSelector?: (results: any, xhr: JQueryXHR) => T, options?: JQueryAjaxSettings, timeToAlert: number = 9000, baseUrl?: string): JQueryPromise<T> {
        const ajax = this.ajax<T>(relativeUrl, args, "GET", db, options, timeToAlert, baseUrl);
        if (resultsSelector) {
            const task = $.Deferred<T>();
            ajax.done((results, status, xhr) => {
                var transformedResults = resultsSelector(results, xhr);
                task.resolve(transformedResults, status, xhr);
            });
            ajax.fail((request, status, error) => {
                task.reject(request, status, error);
                });
            return task;
        } else {
            return ajax;
        }
    }

    protected head<T>(relativeUrl: string, args: any, db?: database, resultsSelector?: (results: any, xhr: JQueryXHR) => T): JQueryPromise<T> {
        const ajax = this.ajax<T>(relativeUrl, args, "HEAD", db);
        if (resultsSelector) {
            const task = $.Deferred<T>();
            ajax.done((results, status, xhr) => {
                const allHeaders = xhr.getAllResponseHeaders();
                if (allHeaders) {
                    const headersObject = {};
                    const headersArray = xhr.getAllResponseHeaders().trim().split(/\r?\n/);
                    for (let n = 0; n < headersArray.length; n++) {
                        const keyValue = headersArray[n].split(": ");
                        if (keyValue.length === 2) {
                            (<any>headersObject)[keyValue[0]] = keyValue[1];
                        }
                    }
                    const transformedResults = resultsSelector(headersObject, xhr);
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

    protected put<T>(relativeUrl: string, args: any, db?: database, options?: JQueryAjaxSettings, timeToAlert: number = 9000): JQueryPromise<T> {
        return this.ajax<T>(relativeUrl, args, "PUT", db, options, timeToAlert);
    }

    protected reset<T>(relativeUrl: string, args: any, db?: database, options?: JQueryAjaxSettings): JQueryPromise<T> {
        return this.ajax<T>(relativeUrl, args, "RESET", db, options);
    }

    protected del<T>(relativeUrl: string, args: any, db?: database, options?: JQueryAjaxSettings, timeToAlert: number = 9000): JQueryPromise<T> {
        return this.ajax<T>(relativeUrl, args, "DELETE", db, options, timeToAlert);
    }

    protected post<T>(relativeUrl: string, args: any, db?: database, options?: JQueryAjaxSettings, timeToAlert: number = 9000, baseUrl?: string): JQueryPromise<any> {
        return this.ajax<T>(relativeUrl, args, "POST", db, options, timeToAlert, baseUrl);
    }

    protected patch<T>(relativeUrl: string, args: any, db?: database, options?: JQueryAjaxSettings): JQueryPromise<T> {
        return this.ajax<T>(relativeUrl, args, "PATCH", db, options);
    }

    protected ajax<T>(relativeUrl: string, args: any, method: string, db?: database, options?: JQueryAjaxSettings, timeToAlert: number = 9000, baseUrl?: string): JQueryPromise<T> {
        const requestExecution = protractedCommandsDetector.instance.requestStarted(4000, timeToAlert);

        // ContentType:
        //
        // Can't use application/json in cross-domain requests, otherwise it 
        // issues OPTIONS preflight request first, which doesn't return proper 
        // headers(e.g.Etag header, etc.)
        // 
        // So, for GETs, we issue text/plain requests, which skip the OPTIONS
        // request and goes straight for the GET request.
        const contentType = method === "GET" ?
            "text/plain; charset=utf-8" :
            "application/json; charset=utf-8";

        const url = baseUrl ? baseUrl + appUrl.forDatabaseQuery(db) + relativeUrl : appUrl.forDatabaseQuery(db) + relativeUrl;

        const xhrConfiguration = (xhr: XMLHttpRequest) => {
            xhr.upload.addEventListener("progress", (evt: ProgressEvent) => {
                if (evt.lengthComputable) {
                    const percentComplete = (evt.loaded / evt.total) * 100;
                    if (percentComplete < 100) {
                        requestExecution.markProgress();
                    }
                }
            }, false);
        };
        
        const defaultOptions = {
            url: url,
            data: args,
            dataType: "json",
            contentType: contentType, 
            type: method,
            headers: <any>undefined,
            xhr: () => {
                const xhr = new XMLHttpRequest();
                xhrConfiguration(xhr);
                return xhr;
            }
        };
        
        if (options) {
            for (let prop in options) {
                if (prop === "xhr") {
                    (defaultOptions as any)["xhr"] = () => {
                        const xhrFactory = (<any>options)["xhr"];
                        const xhr = xhrFactory();
                        xhrConfiguration(xhr);
                        return xhr;
                    }
                } else {
                    (<any>defaultOptions)[prop] = (<any>options)[prop];    
                }
            }
        }

        defaultOptions.headers = defaultOptions.headers || {};
        defaultOptions.headers[commandBase.ravenStudioVersionHeader] = commandBase.ravenClientVersion;

        return $.ajax(defaultOptions)
            .always(() => {
                requestExecution.markCompleted();
            });
    }

    protected extractEtag(xhr: JQueryXHR) {
        let etag = xhr.getResponseHeader("ETag");

        if (!etag) {
            return null;
        }

        if (etag.startsWith('"')) {
            etag = etag.substr(1);
        }

        if (etag.endsWith('"')) {
            etag = etag.substr(0, etag.length - 1);
        }
        return etag;
    }

    reportInfo(title: string, details?: string) {
        messagePublisher.reportInfo(title, details);
    }

    reportError(title: string, details?: string, httpStatusText?: string) {
        messagePublisher.reportError(title, details, httpStatusText);
    }

    reportSuccess(title: string, details?: string) {
        messagePublisher.reportSuccess(title, details);
    }

    reportWarning(title: string, details?: string, httpStatusText?: string) {
        messagePublisher.reportWarning(title, details, httpStatusText);
    }

    static getOptionsForImport(isUploading: KnockoutObservable<boolean>, uploadStatus: KnockoutObservable<number>) : JQueryAjaxSettings {
        const options: JQueryAjaxSettings = {
            processData: false, // Prevents JQuery from automatically transforming the data into a query string. http://api.jquery.com/jQuery.ajax/
            contentType: false,
            cache: false,
            dataType: "",
            xhr: () => {
                const xhr = new XMLHttpRequest();
                xhr.upload.addEventListener("progress", (event: ProgressEvent) => {
                    if (!isUploading() || !event.lengthComputable) {
                        return;
                    }

                    const percentComplete = (event.loaded / event.total) * 100;
                    if (percentComplete === 100) {
                        setTimeout(() => isUploading(false), 700);
                    }

                    uploadStatus(percentComplete);
                }, false);

                return xhr;
            }
        };

        return options;
    }
}

export = commandBase;

