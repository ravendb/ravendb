var __extends = this.__extends || function (d, b) {
    for (var p in b) if (b.hasOwnProperty(p)) d[p] = b[p];
    function __() { this.constructor = d; }
    __.prototype = b.prototype;
    d.prototype = new __();
};
define(["require", "exports", "commands/commandBase", "models/document", "models/database"], function(require, exports, __commandBase__, __document__, __database__) {
    var commandBase = __commandBase__;
    var document = __document__;
    var database = __database__;

    var saveDocumentCommand = (function (_super) {
        __extends(saveDocumentCommand, _super);
        function saveDocumentCommand(id, document, db) {
            _super.call(this);
            this.id = id;
            this.document = document;
            this.db = db;
        }
        saveDocumentCommand.prototype.execute = function () {
            var _this = this;
            this.reportInfo("Saving " + this.id + "...");

            var customHeaders = {
                'Raven-Client-Version': commandBase.ravenClientVersion,
                'Raven-Entity-Name': this.document.__metadata.ravenEntityName,
                'Raven-Clr-Type': this.document.__metadata.ravenClrType,
                'If-None-Match': this.document.__metadata.etag
            };
            var args = JSON.stringify(this.document.toDto());
            var url = "/docs/" + this.id;
            var saveTask = this.put(url, args, this.db, customHeaders);

            saveTask.done(function () {
                return _this.reportSuccess("Saved " + _this.id);
            });
            saveTask.fail(function (response) {
                return _this.reportError("Failed to save " + _this.id, JSON.stringify(response));
            });
            return saveTask;
        };
        return saveDocumentCommand;
    })(commandBase);

    
    return saveDocumentCommand;
});
//# sourceMappingURL=saveDocumentCommand.js.map
