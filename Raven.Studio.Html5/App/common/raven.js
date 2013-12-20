define(["require", "exports", "models/database", "models/collection", "models/collectionInfo", "models/document", "common/pagedResultSet", "common/appUrl"], function(require, exports, database, collection, collectionInfo, document, pagedResultSet, appUrl) {
    var raven = (function () {
        function raven() {
        }
        raven.prototype.userInfo = function () {
            this.requireActiveDatabase();
            var url = "/debug/user-info";
            return this.fetch(url, null, raven.activeDatabase(), null);
        };

        raven.prototype.document = function (id) {
            var resultsSelector = function (dto) {
                return new document(dto);
            };
            var url = "/docs/" + encodeURIComponent(id);
            return this.fetch(url, null, raven.activeDatabase(), resultsSelector);
        };

        raven.prototype.documentWithMetadata = function (id) {
            var resultsSelector = function (dtoResults) {
                return new document(dtoResults[0]);
            };
            return this.docsById(id, 0, 1, false, resultsSelector);
        };

        raven.prototype.searchIds = function (searchTerm, start, pageSize, metadataOnly) {
            var resultsSelector = function (dtoResults) {
                return dtoResults.map(function (dto) {
                    return new document(dto);
                });
            };
            return this.docsById(searchTerm, start, pageSize, metadataOnly, resultsSelector);
        };

        raven.prototype.getDatabaseUrl = function (database) {
            if (database && !database.isSystem) {
                return appUrl.baseUrl + "/databases/" + database.name;
            }

            return appUrl.baseUrl;
        };

        // TODO: This doesn't really belong here.
        raven.getEntityNameFromId = function (id) {
            if (!id) {
                return null;
            }

            // TODO: is there a better way to do this?
            var slashIndex = id.lastIndexOf('/');
            if (slashIndex >= 1) {
                return id.substring(0, slashIndex);
            }

            return id;
        };

        raven.prototype.docsById = function (idOrPartialId, start, pageSize, metadataOnly, resultsSelector) {
            var url = "/docs/";
            var args = {
                startsWith: idOrPartialId,
                start: start,
                pageSize: pageSize
            };
            return this.fetch(url, args, raven.activeDatabase(), resultsSelector);
        };

        raven.prototype.requireActiveDatabase = function () {
            if (!raven.activeDatabase()) {
                throw new Error("Must have an active database before calling this method.");
            }
        };

        raven.prototype.fetch = function (relativeUrl, args, database, resultsSelector) {
            var ajax = this.ajax(relativeUrl, args, "GET", database);
            if (resultsSelector) {
                var task = $.Deferred();
                ajax.done(function (results) {
                    var transformedResults = resultsSelector(results);
                    task.resolve(transformedResults);
                });
                ajax.fail(function (request, status, error) {
                    return task.reject(request, status, error);
                });
                return task;
            } else {
                return ajax;
            }
        };

        raven.prototype.post = function (relativeUrl, args, database, customHeaders) {
            return this.ajax(relativeUrl, args, "POST", database, customHeaders);
        };

        raven.prototype.put = function (relativeUrl, args, database, customHeaders) {
            return this.ajax(relativeUrl, args, "PUT", database, customHeaders);
        };

        raven.prototype.delete_ = function (relativeUrl, args, database, customHeaders) {
            return this.ajax(relativeUrl, args, "DELETE", database, customHeaders);
        };

        raven.prototype.ajax = function (relativeUrl, args, method, database, customHeaders) {
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
        raven.ravenClientVersion = '3.0.0.0';
        raven.activeDatabase = ko.observable().subscribeTo("ActivateDatabase");
        return raven;
    })();

    
    return raven;
});
//# sourceMappingURL=raven.js.map
