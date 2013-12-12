var __extends = this.__extends || function (d, b) {
    for (var p in b) if (b.hasOwnProperty(p)) d[p] = b[p];
    function __() { this.constructor = d; }
    __.prototype = b.prototype;
    d.prototype = new __();
};
define(["require", "exports", "commands/commandBase", "models/collectionInfo"], function(require, exports, commandBase, collectionInfo) {
    var getCollectionInfoCommand = (function (_super) {
        __extends(getCollectionInfoCommand, _super);
        function getCollectionInfoCommand(collection, db) {
            _super.call(this);
            this.collection = collection;
            this.db = db;
        }
        getCollectionInfoCommand.prototype.execute = function () {
            var args = {
                query: "Tag:" + (this.collection.isAllDocuments ? '' : this.collection.name),
                start: 0,
                pageSize: 0
            };

            var resultsSelector = function (dto) {
                return new collectionInfo(dto);
            };
            var url = "/indexes/Raven/DocumentsByEntityName";
            return this.query(url, args, this.db, resultsSelector);
        };
        return getCollectionInfoCommand;
    })(commandBase);

    
    return getCollectionInfoCommand;
});
//# sourceMappingURL=getCollectionInfoCommand.js.map
