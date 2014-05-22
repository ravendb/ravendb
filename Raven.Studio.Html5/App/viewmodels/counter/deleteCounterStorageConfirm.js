var __extends = this.__extends || function (d, b) {
    for (var p in b) if (b.hasOwnProperty(p)) d[p] = b[p];
    function __() { this.constructor = d; }
    __.prototype = b.prototype;
    d.prototype = new __();
};
define(["require", "exports", "commands/counter/deleteCounterStorageCommand", "plugins/dialog", "viewmodels/dialogViewModelBase"], function(require, exports, deleteCounterStorageCommand, dialog, dialogViewModelBase) {
    var deleteCounterStorageConfirm = (function (_super) {
        __extends(deleteCounterStorageConfirm, _super);
        function deleteCounterStorageConfirm(storageToDelete) {
            _super.call(this);
            this.storageToDelete = storageToDelete;
            this.isKeepingFiles = ko.observable(true);
            this.deleteTask = $.Deferred();

            if (!storageToDelete) {
                throw new Error("Must specified counter storage to delete.");
            }
        }
        deleteCounterStorageConfirm.prototype.keepFiles = function () {
            this.isKeepingFiles(true);
        };

        deleteCounterStorageConfirm.prototype.deleteEverything = function () {
            this.isKeepingFiles(false);
        };

        deleteCounterStorageConfirm.prototype.deleteCounterStorage = function () {
            var _this = this;
            new deleteCounterStorageCommand(this.storageToDelete.name, this.isKeepingFiles() === false).execute().done(function (results) {
                return _this.deleteTask.resolve(results);
            }).fail(function (details) {
                return _this.deleteTask.reject(details);
            });

            dialog.close(this);
        };

        deleteCounterStorageConfirm.prototype.cancel = function () {
            this.deleteTask.reject();
            dialog.close(this);
        };
        return deleteCounterStorageConfirm;
    })(dialogViewModelBase);

    
    return deleteCounterStorageConfirm;
});
//# sourceMappingURL=deleteCounterStorageConfirm.js.map
