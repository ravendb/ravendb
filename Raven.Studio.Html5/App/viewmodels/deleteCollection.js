define(["require", "exports", "models/document", "plugins/dialog", "commands/deleteCollectionCommand", "models/collection"], function(require, exports, __document__, __dialog__, __deleteCollectionCommand__, __collection__) {
    var document = __document__;
    var dialog = __dialog__;
    var deleteCollectionCommand = __deleteCollectionCommand__;
    var collection = __collection__;

    var deleteCollection = (function () {
        function deleteCollection(collection) {
            this.collection = collection;
            this.deletionTask = $.Deferred();
            this.deletionStarted = false;
        }
        deleteCollection.prototype.deleteCollection = function () {
            var _this = this;
            var deleteCommand = new deleteCollectionCommand(this.collection.name);
            var deleteCommandTask = deleteCommand.execute();
            deleteCommandTask.done(function () {
                return _this.deletionTask.resolve(_this.collection);
            });
            deleteCommandTask.fail(function (response) {
                return _this.deletionTask.reject(response);
            });
            this.deletionStarted = true;
            dialog.close(this);
        };

        deleteCollection.prototype.cancel = function () {
            dialog.close(this);
        };

        deleteCollection.prototype.deactivate = function () {
            if (!this.deletionStarted) {
                this.deletionTask.reject();
            }
        };
        return deleteCollection;
    })();

    
    return deleteCollection;
});
//# sourceMappingURL=deleteCollection.js.map
