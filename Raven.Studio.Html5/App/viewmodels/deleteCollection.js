define(["require", "exports", "models/document", "plugins/dialog", "commands/deleteCollectionCommand", "models/collection"], function(require, exports, document, dialog, deleteCollectionCommand, collection) {
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
            // If we were closed via X button or other dialog dismissal, reject the deletion task since
            // we never started it.
            if (!this.deletionStarted) {
                this.deletionTask.reject();
            }
        };
        return deleteCollection;
    })();

    
    return deleteCollection;
});
//# sourceMappingURL=deleteCollection.js.map
