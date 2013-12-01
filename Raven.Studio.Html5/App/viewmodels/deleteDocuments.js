define(["require", "exports", "models/document", "plugins/dialog", "commands/deleteDocumentsCommand"], function(require, exports, __document__, __dialog__, __deleteDocumentsCommand__) {
    var document = __document__;
    var dialog = __dialog__;
    var deleteDocumentsCommand = __deleteDocumentsCommand__;

    var deleteDocuments = (function () {
        function deleteDocuments(documents) {
            this.documents = ko.observableArray();
            this.deletionStarted = false;
            this.deletionTask = $.Deferred();
            if (documents.length === 0) {
                throw new Error("Must have at least one document to delete.");
            }

            this.documents(documents);
        }
        deleteDocuments.prototype.deleteDocs = function () {
            var _this = this;
            var deletedDocIds = this.documents().map(function (i) {
                return i.getId();
            });
            var deleteCommand = new deleteDocumentsCommand(deletedDocIds);
            var deleteCommandTask = deleteCommand.execute();

            deleteCommandTask.done(function () {
                return _this.deletionTask.resolve(_this.documents());
            });
            deleteCommandTask.fail(function (response) {
                return _this.deletionTask.reject(response);
            });

            this.deletionStarted = true;
            dialog.close(this);
        };

        deleteDocuments.prototype.cancel = function () {
            dialog.close(this);
        };

        deleteDocuments.prototype.deactivate = function (args) {
            if (!this.deletionStarted) {
                this.deletionTask.reject();
            }
        };
        return deleteDocuments;
    })();

    
    return deleteDocuments;
});
//# sourceMappingURL=deleteDocuments.js.map
