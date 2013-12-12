var __extends = this.__extends || function (d, b) {
    for (var p in b) if (b.hasOwnProperty(p)) d[p] = b[p];
    function __() { this.constructor = d; }
    __.prototype = b.prototype;
    d.prototype = new __();
};
define(["require", "exports", "commands/commandBase", "models/database"], function(require, exports, __commandBase__, __database__) {
    var commandBase = __commandBase__;
    var database = __database__;

    var deleteDocumentsCommand = (function (_super) {
        __extends(deleteDocumentsCommand, _super);
        function deleteDocumentsCommand(docIds, db) {
            _super.call(this);
            this.docIds = docIds;
            this.db = db;
        }
        deleteDocumentsCommand.prototype.execute = function () {
            var _this = this;
            var deleteDocs = this.docIds.map(function (id) {
                return _this.createDeleteDocument(id);
            });
            var deleteTask = this.post("/bulk_docs", ko.toJSON(deleteDocs), this.db);

            var docCount = this.docIds.length;
            var alertInfoTitle = docCount > 1 ? "Deleting " + docCount + "docs..." : "Deleting " + this.docIds[0];
            this.reportInfo(alertInfoTitle);

            deleteTask.done(function () {
                return _this.reportSuccess("Deleted " + docCount + " docs");
            });
            deleteTask.fail(function (response) {
                return _this.reportError("Failed to delete docs", JSON.stringify(response));
            });

            return deleteTask;
        };

        deleteDocumentsCommand.prototype.createDeleteDocument = function (id) {
            return {
                Key: id,
                Method: "DELETE",
                Etag: null,
                AdditionalData: null
            };
        };
        return deleteDocumentsCommand;
    })(commandBase);

    
    return deleteDocumentsCommand;
});
//# sourceMappingURL=deleteDocumentsCommand.js.map
