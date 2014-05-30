var __extends = this.__extends || function (d, b) {
    for (var p in b) if (b.hasOwnProperty(p)) d[p] = b[p];
    function __() { this.constructor = d; }
    __.prototype = b.prototype;
    d.prototype = new __();
};
define(["require", "exports", "commands/commandBase"], function(require, exports, commandBase) {
    var deleteCounterStorageCommand = (function (_super) {
        __extends(deleteCounterStorageCommand, _super);
        function deleteCounterStorageCommand(counterStorageName, isHardDelete) {
            _super.call(this);
            this.counterStorageName = counterStorageName;
            this.isHardDelete = isHardDelete;
        }
        deleteCounterStorageCommand.prototype.execute = function () {
            var _this = this;
            this.reportInfo("Deleting " + this.counterStorageName + "...");

            var url = "/counterstorage/admin/" + encodeURIComponent(this.counterStorageName) + "?hard-delete=" + this.isHardDelete;
            var deleteTask = this.del(url, null, null, { dataType: undefined });
            deleteTask.fail(function (response) {
                return _this.reportError("Failed to delete counter storage", response.responseText, response.statusText);
            });
            deleteTask.done(function () {
                return _this.reportSuccess("Deleted " + _this.counterStorageName);
            });
            return deleteTask;
        };
        return deleteCounterStorageCommand;
    })(commandBase);

    
    return deleteCounterStorageCommand;
});
//# sourceMappingURL=deleteCounterStorageCommand.js.map
