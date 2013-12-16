var __extends = this.__extends || function (d, b) {
    for (var p in b) if (b.hasOwnProperty(p)) d[p] = b[p];
    function __() { this.constructor = d; }
    __.prototype = b.prototype;
    d.prototype = new __();
};
define(["require", "exports", "commands/commandBase", "common/alertType", "models/database"], function(require, exports, __commandBase__, __alertType__, __database__) {
    var commandBase = __commandBase__;
    var alertType = __alertType__;
    var database = __database__;

    var deleteCollectionCommand = (function (_super) {
        __extends(deleteCollectionCommand, _super);
        function deleteCollectionCommand(collectionName, db) {
            _super.call(this);
            this.collectionName = collectionName;
            this.db = db;
        }
        deleteCollectionCommand.prototype.execute = function () {
            var _this = this;
            this.reportInfo("Deleting " + this.collectionName + " collection...");

            var args = {
                query: "Tag:" + this.collectionName,
                pageSize: 128,
                allowStale: true
            };
            var url = "/bulk_docs/Raven/DocumentsByEntityName";
            var urlParams = "?query=Tag%3A" + encodeURIComponent(this.collectionName) + "&pageSize=128&allowStale=true";
            var deleteTask = this.del(url + urlParams, null, this.db);
            deleteTask.done(function () {
                return _this.reportSuccess("Deleted " + _this.collectionName + " collection");
            });
            deleteTask.fail(function (response) {
                return _this.reportError("Failed to delete collection", JSON.stringify(response));
            });
            return deleteTask;
        };
        return deleteCollectionCommand;
    })(commandBase);

    
    return deleteCollectionCommand;
});
//# sourceMappingURL=deleteCollectionCommand.js.map
