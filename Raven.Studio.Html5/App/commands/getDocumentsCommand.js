var __extends = this.__extends || function (d, b) {
    for (var p in b) if (b.hasOwnProperty(p)) d[p] = b[p];
    function __() { this.constructor = d; }
    __.prototype = b.prototype;
    d.prototype = new __();
};
define(["require", "exports", "commands/commandBase", "models/database", "models/collectionInfo", "models/collection", "common/pagedResultSet"], function(require, exports, commandBase, database, collectionInfo, collection, pagedResultSet) {
    var getDocumentsCommand = (function (_super) {
        __extends(getDocumentsCommand, _super);
        function getDocumentsCommand(collection, db, skip, take) {
            _super.call(this);
            this.collection = collection;
            this.db = db;
            this.skip = skip;
            this.take = take;
        }
        getDocumentsCommand.prototype.execute = function () {
            var args = {
                query: "Tag:" + this.collection.isAllDocuments ? '' : this.collection.name,
                start: this.skip,
                pageSize: this.take
            };

            var resultsSelector = function (dto) {
                return new collectionInfo(dto);
            };
            var url = "/indexes/Raven/DocumentsByEntityName";
            var documentsTask = $.Deferred();
            this.query(url, args, this.db, resultsSelector).then(function (collection) {
                var items = collection.results;
                var resultSet = new pagedResultSet(items, collection.totalResults);
                documentsTask.resolve(resultSet);
            });
            return documentsTask;
        };
        return getDocumentsCommand;
    })(commandBase);

    
    return getDocumentsCommand;
});
//# sourceMappingURL=getDocumentsCommand.js.map
