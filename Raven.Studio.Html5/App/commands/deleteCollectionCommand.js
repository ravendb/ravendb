var __extends = this.__extends || function (d, b) {
    for (var p in b) if (b.hasOwnProperty(p)) d[p] = b[p];
    function __() { this.constructor = d; }
    __.prototype = b.prototype;
    d.prototype = new __();
};
define(["require", "exports", "commands/commandBase", "common/alertType"], function(require, exports, __commandBase__, __alertType__) {
    var commandBase = __commandBase__;
    var alertType = __alertType__;

    var deleteCollectionCommand = (function (_super) {
        __extends(deleteCollectionCommand, _super);
        function deleteCollectionCommand(collectionName) {
            _super.call(this);
            this.collectionName = collectionName;
        }
        deleteCollectionCommand.prototype.execute = function () {
            var _this = this;
            var deleteTask = this.ravenDb.deleteCollection(this.collectionName);

            this.reportInfo("Deleting " + this.collectionName + " collection...");

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
