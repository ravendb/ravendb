/// <reference path="../models/dto.ts" />

import alertType = require("common/alertType");
import messagePublisher = require("common/messagePublisher");
import database = require("models/resources/database");
import resource = require("models/resources/resource");
import appUrl = require("common/appUrl");
import oauthContext = require("common/oauthContext");
import forge = require("forge/forge_custom.min");
import router = require("plugins/router");

/// Commands encapsulate a read or write operation to the database and support progress notifications and common AJAX related functionality.
class commandBase {

    // TODO: better place for this?
    static ravenClientVersion = '3.5.0.0';
    static splashTimerHandle = 0;
    static alertTimeout = 0;
    static loadingCounter = 0;
    static biggestTimeToAlert = 0;

    execute<T>(): JQueryPromise<T> {
        throw new Error("Execute must be overridden.");
    }

    urlEncodeArgs(args: any): string {
        return appUrl.urlEncodeArgs(args);
    }

    getTimeToAlert(longWait: boolean) {
        return longWait ? 60000 : 9000;
    }

    query<T>(relativeUrl: string, args: any, resource?: resource, resultsSelector?: (results: any) => T, options?: JQueryAjaxSettings, timeToAlert: number = 9000): JQueryPromise<T> {
        var ajax = this.ajax(relativeUrl, args, "GET", resource, options, timeToAlert);
        if (resultsSelector) {
            var task = $.Deferred();
            ajax.done((results, status, xhr) => {
                //if we fetched a database document, save the etag from the header
                if (results.hasOwnProperty('SecuredSettings')) {
                    results['__metadata'] = { '@etag': xhr.getResponseHeader('Etag') };
                }
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
                            //keyValue[1] = keyValue[1].replaceAll("\"", "");
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

    put(relativeUrl: string, args: any, resource?: resource, options?: JQueryAjaxSettings, timeToAlert: number = 9000): JQueryPromise<any> {
        return this.ajax(relativeUrl, args, "PUT", resource, options, timeToAlert);
    }

    reset(relativeUrl: string, args: any, resource?: resource, options?: JQueryAjaxSettings, timeToAlert: number = 9000): JQueryPromise<any> {
        return this.ajax(relativeUrl, args, "RESET", resource, options, timeToAlert);
    }

    /*
     * Performs a DELETE rest call.
    */
    del(relativeUrl: string, args: any, resource?: resource, options?: JQueryAjaxSettings, timeToAlert: number = 9000): JQueryPromise<any> {
        return this.ajax(relativeUrl, args, "DELETE", resource, options, timeToAlert);
    }

    post(relativeUrl: string, args: any, resource?: resource, options?: JQueryAjaxSettings, timeToAlert: number = 9000): JQueryPromise<any> {
        return this.ajax(relativeUrl, args, "POST", resource, options, timeToAlert);
    }

    patch(relativeUrl: string, args: any, resource?: resource, options?: JQueryAjaxSettings): JQueryPromise<any> {
        return this.ajax(relativeUrl, args, "PATCH", resource, options);
    }

    evalJs(relativeUrl: string, args: any, resource?: resource, options?: JQueryAjaxSettings): JQueryPromise<any> {
        return this.ajax(relativeUrl, args, "EVAL", resource, options);
    }

    private ajax(relativeUrl: string, args: any, method: string, resource?: resource, options?: JQueryAjaxSettings, timeToAlert: number = 9000): JQueryPromise<any> {
        var originalArguments = arguments;
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
            url: appUrl.forResourceQuery(resource) + relativeUrl,
            data: args,
            dataType: "json",
            contentType: contentType, 
            type: method,
            headers: undefined,
            xhr: () => {
                var xhr = new XMLHttpRequest();
                xhr.upload.addEventListener("progress", (evt: ProgressEvent) => {
                    if (evt.lengthComputable) {
                        var percentComplete = (evt.loaded / evt.total) * 100;
                        if (percentComplete < 100) {
                            // waiting for upload progress to complete
                            clearTimeout(commandBase.alertTimeout);
                            commandBase.alertTimeout = setTimeout(commandBase.showServerNotRespondingAlert, timeToAlert);
                        }
                        ko.postbox.publish("UploadProgress", percentComplete);
                    }
                }, false);

                return xhr;
            }
        };
        
        if (options) {
            for (var prop in options) {
                defaultOptions[prop] = options[prop];
            }
        }

        var isBiggestTimeToAlertUpdated = timeToAlert > commandBase.biggestTimeToAlert;
        if ((commandBase.loadingCounter == 0 && timeToAlert > 0) || isBiggestTimeToAlertUpdated) {
            commandBase.biggestTimeToAlert = timeToAlert;
            clearTimeout(commandBase.splashTimerHandle);
            commandBase.splashTimerHandle = setTimeout(commandBase.showSpin, 1000, timeToAlert, isBiggestTimeToAlertUpdated); 
        }

        if (oauthContext.apiKey()) {
            if (!defaultOptions.headers) {
                var newHeaders: any = {};
                defaultOptions.headers = newHeaders;
            }
            defaultOptions.headers["Has-Api-Key"] = "True";
        }

        if (oauthContext.authHeader()) {
            if (!defaultOptions.headers) {
                var newHeaders: any = {};
                defaultOptions.headers = newHeaders;
            }
            defaultOptions.headers["Authorization"] = oauthContext.authHeader();
        }

        commandBase.loadingCounter++;
        var ajaxTask = $.Deferred();

        $.ajax(defaultOptions).always(() => {
            --commandBase.loadingCounter;
            if (commandBase.loadingCounter == 0) {
                clearTimeout(commandBase.splashTimerHandle);
                clearTimeout(commandBase.alertTimeout);
                commandBase.alertTimeout = 0;
                commandBase.hideSpin();
            }
        }).done((results, status, xhr) => {
            ajaxTask.resolve(results, status, xhr);
        }).fail((request, status, error) => {
            var dbBeingUpdated = request.getResponseHeader("Raven-Database-Load-In-Progress");
            if (dbBeingUpdated) {
                ajaxTask.reject(request, status, error);
                var currentDb = appUrl.getDatabase();
                if (currentDb != null && currentDb.name == dbBeingUpdated) {
                    router.navigate(appUrl.forUpgrade(new database(dbBeingUpdated)));
                }
            } else if (request.status == ResponseCodes.PreconditionFailed && oauthContext.apiKey()) {
                this.handleOAuth(ajaxTask, request, originalArguments);
            } else {
                ajaxTask.reject(request, status, error);
            }
        });

        return ajaxTask.promise();
    }

    handleOAuth(task: JQueryDeferred<any>, request: JQueryXHR, originalArguments: IArguments) {
        var oauthSource = request.getResponseHeader('OAuth-Source');

        // issue request to oauth source endpoint to get RSA exponent and modulus
        $.ajax({
            type: 'POST',
            url: oauthSource,
            headers: {
                grant_type: 'client_credentials'
            }
        }).fail((request, status, error) => {
                if (request.status != ResponseCodes.PreconditionFailed) {
                    task.reject(request, status, error);
                } else {
                    var wwwAuth:string = request.getResponseHeader('WWW-Authenticate');
                    var tokens = wwwAuth.split(',');
                    var authRequest:any = {};
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
                    }).done((results, status, xhr) => {
                        var resultsAsString = JSON.stringify(results, null, 0);
                        oauthContext.authHeader("Bearer " + resultsAsString.replace(/(\r\n|\n|\r)/gm,""));
                        this.retryOriginalRequest(task, originalArguments);
                    }).fail((request, status, error) => {
                        task.reject(request, status, error);
                    });
                }
            });
    }

    retryOriginalRequest(task: JQueryDeferred<any>, orignalArguments: IArguments) {
        this.ajax.apply(this, orignalArguments).done((results, status, xhr) => {
            task.resolve(results, status, xhr);
        }).fail((request, status, error) => {
                task.reject(request, status, error);
        });
    }

    prepareResponse(challenge: string) {
        var input = challenge + ";" + oauthContext.apiKeySecret();
        var md = forge.md.sha1.create();
        md.update(input);
        return forge.util.encode64(md.digest().getBytes());
    }

    objectToString(input: any) {
        return $.map(input, (value, key) => key + "=" + value).join(',');
    }

    base64ToBigInt(input) {
        input = forge.util.decode64(input);
        var hex = forge.util.bytesToHex(input);
        return new forge.jsbn.BigInteger(hex, 16);
    }

    encryptAsymmetric(exponent, modulus, data) {
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

    private static showSpin(timeToAlert: number, isBiggestTimeToAlertUpdated: boolean) {
        ko.postbox.publish("LoadProgress", alertType.warning);
        if (commandBase.alertTimeout == 0 || isBiggestTimeToAlertUpdated) {
            clearTimeout(commandBase.alertTimeout);
            commandBase.biggestTimeToAlert = timeToAlert;
            commandBase.alertTimeout = setTimeout(commandBase.showServerNotRespondingAlert, timeToAlert);
        }
    }

    private static showServerNotRespondingAlert() {
        ko.postbox.publish("LoadProgress", alertType.danger);
    }

    private static hideSpin() {
        ko.postbox.publish("LoadProgress", null);
        $.unblockUI();
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

    reportWarningWithButton(title: string, details: string, buttonName: string, action: () => any) {
        messagePublisher.reportWarningWithButton(title, details, buttonName, action);
    }
}

export = commandBase;
