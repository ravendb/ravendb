var __extends = this.__extends || function (d, b) {
    for (var p in b) if (b.hasOwnProperty(p)) d[p] = b[p];
    function __() { this.constructor = d; }
    __.prototype = b.prototype;
    d.prototype = new __();
};
define(["require", "exports", "commands/commandBase", "models/database", "common/pagedResultSet", "models/document"], function(require, exports, commandBase, database, pagedResultSet, document) {
    /*
    * getAllDocumentsCommand is a specialized command that fetches all the documents in a specified database.
    */
    var getAllDocumentsCommand = (function (_super) {
        __extends(getAllDocumentsCommand, _super);
        function getAllDocumentsCommand(ownerDatabase, skip, take) {
            _super.call(this);
            this.ownerDatabase = ownerDatabase;
            this.skip = skip;
            this.take = take;
        }
        getAllDocumentsCommand.prototype.execute = function () {
            // Getting all documents requires a 2 step process:
            // 1. Fetch /indexes/Raven/DocumentsByEntityName to get the total doc count.
            // 2. Fetch /docs to get the actual documents.
            // Fetching #1 will return a document list, but it won't include the system docs.
            // Therefore, we must fetch /docs as well, which gives us the system docs.
            var docsTask = this.fetchDocs();
            var totalResultsTask = this.fetchTotalResultCount();
            var doneTask = $.Deferred();
            var combinedTask = $.when(docsTask, totalResultsTask);
            combinedTask.done(function (docsResult, resultsCount) {
                return doneTask.resolve(new pagedResultSet(docsResult, resultsCount));
            });
            combinedTask.fail(function (xhr) {
                return doneTask.reject(xhr);
            });
            return doneTask;
        };

        getAllDocumentsCommand.prototype.fetchDocs = function () {
            var args = {
                start: this.skip,
                pageSize: this.take
            };

            var docSelector = function (docs) {
                return docs.map(function (d) {
                    return new document(d);
                });
            };
            return this.query("/docs", args, this.ownerDatabase, docSelector);
        };

        getAllDocumentsCommand.prototype.fetchTotalResultCount = function () {
            var args = {
                query: "",
                start: 0,
                pageSize: 0
            };

            var url = "/indexes/Raven/DocumentsByEntityName";
            var countSelector = function (dto) {
                return dto.TotalResults;
            };
            return this.query(url, args, this.ownerDatabase, countSelector);
        };
        return getAllDocumentsCommand;
    })(commandBase);

    
    return getAllDocumentsCommand;
});
//# sourceMappingURL=getAllDocumentsCommand.js.map
