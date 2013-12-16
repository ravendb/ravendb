import database = require("models/database");
import collection = require("models/collection");
import collectionInfo = require("models/collectionInfo");
import document = require("models/document");
import pagedResultSet = require("common/pagedResultSet");
import appUrl = require("common/appUrl");

class raven {
    
    private static ravenClientVersion = '3.0.0.0';
    public static activeDatabase = ko.observable<database>().subscribeTo("ActivateDatabase");

    public userInfo() {
        this.requireActiveDatabase();
        var url = "/debug/user-info";
        return this.fetch(url, null, raven.activeDatabase(), null);
    }

    public document(id: string): JQueryPromise<document> {
        var resultsSelector = (dto: documentDto) => new document(dto);
        var url = "/docs/" + encodeURIComponent(id);
        return this.fetch(url, null, raven.activeDatabase(), resultsSelector);
    }

    public documentWithMetadata(id: string): JQueryPromise<document> {
        var resultsSelector = (dtoResults: documentDto[]) => new document(dtoResults[0]);
        return this.docsById<document>(id, 0, 1, false, resultsSelector);
    }

    public searchIds(searchTerm: string, start: number, pageSize: number, metadataOnly: boolean) {
        var resultsSelector = (dtoResults: documentDto[]) => dtoResults.map(dto => new document(dto));
        return this.docsById<Array<document>>(searchTerm, start, pageSize, metadataOnly, resultsSelector);
    }

    public deleteCollection(collectionName: string): JQueryPromise<any> {
        var args = {
            query: "Tag:" + collectionName,
            pageSize: 128,
            allowStale: true
        };
        var url = "/bulk_docs/Raven/DocumentsByEntityName";
        var urlParams = "?query=Tag%3A" + encodeURIComponent(collectionName) + "&pageSize=128&allowStale=true";
        return this.delete_(url + urlParams, null, raven.activeDatabase());
    }

    public getDatabaseUrl(database: database) {
        if (database && !database.isSystem) {
            return appUrl.baseUrl + "/databases/" + database.name;
        }

        return appUrl.baseUrl;
    }

    // TODO: This doesn't really belong here.
    public static getEntityNameFromId(id: string): string {
        if (!id) {
            return null;
        }

        // TODO: is there a better way to do this?
        var slashIndex = id.lastIndexOf('/');
        if (slashIndex >= 1) {
            return id.substring(0, slashIndex);
        }

        return id;
    }

    private docsById<T>(idOrPartialId: string, start: number, pageSize: number, metadataOnly: boolean, resultsSelector): JQueryPromise<T> {

        var url = "/docs/";
        var args = {
            startsWith: idOrPartialId,
            start: start,
            pageSize: pageSize
        };
        return this.fetch(url, args, raven.activeDatabase(), resultsSelector);
    }

    private requireActiveDatabase() {
        if (!raven.activeDatabase()) {
            throw new Error("Must have an active database before calling this method.");
        }
    }

    private fetch(relativeUrl: string, args: any, database?: database, resultsSelector?: (results: any) => any): JQueryPromise<any> {
        var ajax = this.ajax(relativeUrl, args, "GET", database);
        if (resultsSelector) {
            var task = $.Deferred();
            ajax.done((results) => {
                var transformedResults = resultsSelector(results);
                task.resolve(transformedResults);
            });
            ajax.fail((request, status, error) => task.reject(request, status, error));
            return task;
        } else {
            return ajax;
        }
    }

    private post(relativeUrl: string, args: any, database?: database, customHeaders?: any): JQueryPromise<any> {
        return this.ajax(relativeUrl, args, "POST", database, customHeaders);
    }

    private put(relativeUrl: string, args: any, database?: database, customHeaders?: any): JQueryPromise<any> {
        return this.ajax(relativeUrl, args, "PUT", database, customHeaders);
    }

    private delete_(relativeUrl: string, args: any, database?: database, customHeaders?: any): JQueryPromise<any> {
        return this.ajax(relativeUrl, args, "DELETE", database, customHeaders);
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
}

export = raven;