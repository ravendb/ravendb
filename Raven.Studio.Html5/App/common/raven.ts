import database = require("models/database");
import collection = require("models/collection");
import collectionInfo = require("models/collectionInfo");
import document = require("models/document");
import pagedResultSet = require("common/pagedResultSet");

class raven {

    private baseUrl = "http://localhost:8080"; // For debugging purposes, uncomment this line to point Raven at an already-running Raven server. Requires the Raven server to have it's config set to <add key="Raven/AccessControlAllowOrigin" value="*" />
    //private baseUrl = ""; // This should be used when serving HTML5 Studio from the server app.

    private static ravenClientVersion = '2.5.0.0';
    public static activeDatabase = ko.observable<database>().subscribeTo("ActivateDatabase");

    public databases(): JQueryPromise<Array<database>> {
        var resultsSelector = (databaseNames: string[]) => databaseNames.map(n => new database(n));
        return this.fetch("/databases", { pageSize: 1024 }, null, resultsSelector);
    }

    public databaseStats(databaseName: string): JQueryPromise<documentStatistics> {
        return this.fetch("/databases/" + databaseName + "/stats", null, null);
    }

    public collections(): JQueryPromise<Array<collection>> {
        this.requireActiveDatabase();

        var args = {
            field: "Tag",
            fromValue: "",
            pageSize: 100
        };
        var resultsSelector = (collectionNames: string[]) => collectionNames.map(n => new collection(n));
        return this.fetch("/terms/Raven/DocumentsByEntityName", args, raven.activeDatabase(), resultsSelector);
    }

    public collectionInfo(collectionName?: string, documentsSkip = 0, documentsTake = 0): JQueryPromise<collectionInfo> {
        this.requireActiveDatabase();

        var args = {
            query: collectionName ? "Tag:" + collectionName : undefined,
            start: documentsSkip,
            pageSize: documentsTake
        };

        var resultsSelector = (dto: collectionInfoDto) => new collectionInfo(dto);
        var url = "/indexes/Raven/DocumentsByEntityName";
        return this.fetch(url, args, raven.activeDatabase(), resultsSelector);
    }

    public userInfo() {
        this.requireActiveDatabase();
        var url = "/debug/user-info";
        return this.fetch(url, null, raven.activeDatabase(), null);
    }

    public documents(collectionName: string, skip = 0, take = 30): JQueryPromise<pagedResultSet> {
        this.requireActiveDatabase();

        var documentsTask = $.Deferred();
        this.collectionInfo(collectionName, skip, take)
            .then(collection => {
                var items = collection.results;
                var resultSet = new pagedResultSet(items, collection.totalResults);
                documentsTask.resolve(resultSet);
            });
        return documentsTask;
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

    public deleteDocuments(ids: string[]): JQueryPromise<any> {
        var deleteDocs = ids.map(id => this.createDeleteDocument(id));
        return this.post("/bulk_docs", ko.toJSON(deleteDocs), raven.activeDatabase());
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

    public saveDocument(id: string, doc: document): JQueryPromise<{ Key: string; ETag: string }> {
        var customHeaders = {
            'Raven-Client-Version': raven.ravenClientVersion,
            'Raven-Entity-Name': doc.__metadata.ravenEntityName,
            'Raven-Clr-Type': doc.__metadata.ravenClrType,
            'If-None-Match': doc.__metadata.etag
        };
        var args = JSON.stringify(doc.toDto());
        var url = "/docs/" + id;
        return this.put(url, args, raven.activeDatabase(), customHeaders);
    }

    public createDatabase(databaseName: string): JQueryPromise<any> {
        if (!databaseName) {
            throw new Error("Database must have a name.");
        }

        var databaseDoc = {
            "Settings": {
                "Raven/DataDir": "~\\Databases\\" + databaseName
            },
            "SecuredSettings": {},
            "Disabled": false
        };

        var createTask = this.put("/admin/databases/" + databaseName, JSON.stringify(databaseDoc), null);

        // Forces creation of standard indexes? Looks like it.
        createTask.done(() => this.fetch("/databases/" + databaseName + "/silverlight/ensureStartup", null, null)); 

        return createTask;
    }

    public getBaseUrl() {
        return this.baseUrl;
    }

    public getDatabaseUrl(database: database) {
        if (database && !database.isSystem) {
            return this.baseUrl + "/databases/" + database.name;
        }

        return this.baseUrl;
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

    private createDeleteDocument(id: string) {
        return {
            Key: id,
            Method: "DELETE",
            Etag: null,
            AdditionalData: null
        }
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