var __extends = this.__extends || function (d, b) {
    for (var p in b) if (b.hasOwnProperty(p)) d[p] = b[p];
    function __() { this.constructor = d; }
    __.prototype = b.prototype;
    d.prototype = new __();
};
define(["require", "exports", "commands/commandBase", "models/database", "models/collectionInfo", "models/collection", "common/pagedResultSet"], function(require, exports, __commandBase__, __database__, __collectionInfo__, __collection__, __pagedResultSet__) {
    var commandBase = __commandBase__;
    var database = __database__;
    var collectionInfo = __collectionInfo__;
    var collection = __collection__;
    var pagedResultSet = __pagedResultSet__;

    var getDocumentsInCollection = (function (_super) {
        __extends(getDocumentsInCollection, _super);
        function getDocumentsInCollection(collection, db, skip, take) {
            _super.call(this);
            this.collection = collection;
            this.db = db;
            this.skip = skip;
            this.take = take;
        }
        getDocumentsInCollection.prototype.execute = function () {
            var args = {
                query: this.collection.isAllDocuments ? "Tag:" + null : this.collection.name
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
        return getDocumentsInCollection;
    })(commandBase);

    
    return getDocumentsInCollection;
});
//# sourceMappingURL=getDocumentsInCollection.js.map
