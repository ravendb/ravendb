/// <reference path="../../typings/tsd.d.ts" />

import messagePublisher = require("common/messagePublisher");
import database = require("models/resources/database");
import appUrl = require("common/appUrl");
import oauthContext = require("common/oauthContext");
import forge = require("forge");
import protractedCommandsDetector = require("common/notifications/protractedCommandsDetector");

/// Commands encapsulate a read or write operation to the database and support progress notifications and common AJAX related functionality.
class commandBase {

    // TODO: better place for this?
    static ravenClientVersion = '4.0.0.0';
    private oauthHandler = new oauthHandler(); //TODO: consider static handler?

    execute<T>(): JQueryPromise<T> {
        throw new Error("Execute must be overridden.");
    }

    urlEncodeArgs(args: any): string {
        return appUrl.urlEncodeArgs(args);
    }

    getTimeToAlert(longWait: boolean) {
        return longWait ? 60000 : 9000;
    }

    query<T>(relativeUrl: string, args: any, db?: database, resultsSelector?: (results: any, xhr: JQueryXHR) => T, options?: JQueryAjaxSettings, timeToAlert: number = 9000): JQueryPromise<T> {
        const ajax = this.ajax<T>(relativeUrl, args, "GET", db, options, timeToAlert);
        if (resultsSelector) {
            var task = $.Deferred<T>();
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
        var ajax = this.ajax<T>(relativeUrl, args, "HEAD", db);
        if (resultsSelector) {
            var task = $.Deferred();
            ajax.done((results, status, xhr) => {
                var allHeaders = xhr.getAllResponseHeaders();
                if (allHeaders) {
                    var headersObject = {};
                    var headersArray = xhr.getAllResponseHeaders().trim().split(/\r?\n/);
                    for (var n = 0; n < headersArray.length; n++) {
                        var keyValue = headersArray[n].split(": ");
                        if (keyValue.length === 2) {
                            (<any>headersObject)[keyValue[0]] = keyValue[1];
                        }
                    }
                    var transformedResults = resultsSelector(headersObject, xhr);
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

    protected post<T>(relativeUrl: string, args: any, db?: database, options?: JQueryAjaxSettings, timeToAlert: number = 9000): JQueryPromise<any> {
        return this.ajax<T>(relativeUrl, args, "POST", db, options, timeToAlert);
    }

    protected patch<T>(relativeUrl: string, args: any, db?: database, options?: JQueryAjaxSettings): JQueryPromise<T> {
        return this.ajax<T>(relativeUrl, args, "PATCH", db, options);
    }

    protected ajax<T>(relativeUrl: string, args: any, method: string, db?: database, options?: JQueryAjaxSettings, timeToAlert: number = 9000): JQueryPromise<T> {
        var originalArguments = arguments;

        const requestExecution = protractedCommandsDetector.instance.requestStarted(4000, timeToAlert);

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
            url: appUrl.forDatabaseQuery(db) + relativeUrl,
            data: args,
            dataType: "json",
            contentType: contentType, 
            type: method,
            headers: <any>undefined,
            xhr: () => {
                var xhr = new XMLHttpRequest();
                xhr.upload.addEventListener("progress", (evt: ProgressEvent) => {
                    if (evt.lengthComputable) {
                        var percentComplete = (evt.loaded / evt.total) * 100;
                        if (percentComplete < 100) {
                            requestExecution.markProgress();
                        }
                        //TODO: use event
                        ko.postbox.publish("UploadProgress", percentComplete);
                    }
                }, false);

                return xhr;
            }
        };
        
        if (options) {
            for (var prop in options) {
                (<any>defaultOptions)[prop] = (<any>options)[prop];
            }
        }

        if (oauthContext.apiKey()) { //TODO: move to oauth handler!
            if (!defaultOptions.headers) {
                var newHeaders: any = {};
                defaultOptions.headers = newHeaders;
            }
            defaultOptions.headers["Has-Api-Key"] = "True"; 
        }

        if (oauthContext.authHeader()) { //TODO: move to oauth handler
            if (!defaultOptions.headers) {
                var newHeaders: any = {};
                defaultOptions.headers = newHeaders;
            }
            defaultOptions.headers["Authorization"] = oauthContext.authHeader();
        }

        var ajaxTask = $.Deferred();

        $.ajax(defaultOptions).always(() => {
            requestExecution.markCompleted();
        }).done((results, status, xhr) => {
            ajaxTask.resolve(results, status, xhr);
        }).fail((request, status, error) => {
            var dbBeingUpdated = request.getResponseHeader("Raven-Database-Load-In-Progress");
            if (dbBeingUpdated) {
                ajaxTask.reject(request, status, error);
                /* TODO
                var currentDb = appUrl.getDatabase();
                if (currentDb != null && currentDb.name === dbBeingUpdated) {
                    router.navigate(appUrl.forUpgrade(new database(dbBeingUpdated, false, []))); //TODO: use database manger to get this database!
                }*/
            } else if (request.status === ResponseCodes.PreconditionFailed && oauthContext.apiKey()) {
                this.oauthHandler.handleOAuth(ajaxTask, request, () => this.retryOriginalRequest(ajaxTask, originalArguments));
            } else {
                ajaxTask.reject(request, status, error);
            }
        });

        return ajaxTask.promise();
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

    private retryOriginalRequest(task: JQueryDeferred<any>, orignalArguments: IArguments) {
        this.ajax.apply(this, orignalArguments).done((results: any, status: any, xhr: any) => {
            task.resolve(results, status, xhr);
        }).fail((request: any, status: any, error: any) => {
            task.reject(request, status, error);
        });
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
}

class oauthHandler {
    handleOAuth(task: JQueryDeferred<any>, request: JQueryXHR, retry: Function) {
        var oauthSource = request.getResponseHeader('OAuth-Source');

        // issue request to oauth source endpoint to get RSA exponent and modulus
        $.ajax({
            type: 'POST',
            url: oauthSource,
            headers: {
                grant_type: 'client_credentials'
            }
        }).fail((request, status, error) => {
            if (request.status !== ResponseCodes.PreconditionFailed) {
                task.reject(request, status, error);
            } else {
                var wwwAuth: string = request.getResponseHeader('WWW-Authenticate');
                var tokens = wwwAuth.split(',');
                var authRequest: any = {};
                tokens.forEach(token => {
                    var eqPos = token.indexOf("=");
                    var kv = [token.substring(0, eqPos), token.substring(eqPos + 1)];
                    var m = kv[0].match(/[a-zA-Z]+$/g);
                    if (m) {
                        authRequest[m[0]] = kv[1];
                    } else {
                        authRequest[kv[0]] = kv[1];
                    }
                });

                // form oauth request

                var data = this.objectToString({
                    exponent: authRequest.exponent,
                    modulus: authRequest.modulus,
                    data: this.encryptAsymmetric(authRequest.exponent, authRequest.modulus, this.objectToString({
                        "api key name": oauthContext.apiKeyName(),
                        "challenge": authRequest.challenge,
                        "response": this.prepareResponse(authRequest.challenge)
                    }))
                });

                $.ajax({
                    type: 'POST',
                    url: oauthSource,
                    data: data,
                    headers: {
                        grant_type: 'client_credentials'
                    }
                }).done(results => {
                    var resultsAsString = JSON.stringify(results, null, 0);
                    oauthContext.authHeader("Bearer " + resultsAsString.replace(/(\r\n|\n|\r)/gm, ""));
                    retry();
                }).fail((request, status, error) => {
                    task.reject(request, status, error);
                });
            }
        });
    }

    objectToString(input: any) {
        return $.map(input, (value, key) => key + "=" + value).join(',');
    }

    prepareResponse(challenge: string) {
        var input = challenge + ";" + oauthContext.apiKeySecret();
        var md = forge.md.sha1.create();
        md.update(input);
        return forge.util.encode64(md.digest().getBytes());
    }

    encryptAsymmetric(exponent: any, modulus: any, data: any) {
        var e = this.base64ToBigInt(exponent);
        var n = this.base64ToBigInt(modulus);
        var rsa = forge.pki.rsa;
        var publicKey = rsa.setPublicKey(n, e);

        var key = forge.random.getBytesSync(32);
        var iv = forge.random.getBytesSync(16);

        var keyAndIvEncrypted = publicKey.encrypt(key + iv, 'RSA-OAEP');

        var cipher = forge.cipher.createCipher('AES-CBC', key);
        cipher.start({ iv: iv });
        cipher.update(forge.util.createBuffer(data));
        cipher.finish();
        var encrypted = cipher.output;
        return forge.util.encode64(keyAndIvEncrypted + encrypted.data);
    }

    base64ToBigInt(input: any) {
        input = forge.util.decode64(input);
        var hex = forge.util.bytesToHex(input);
        return new forge.jsbn.BigInteger(hex, 16);
    }
}

export = commandBase;

