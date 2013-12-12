define(["require", "exports", "models/document", "plugins/dialog", "commands/createDatabaseCommand", "models/collection"], function(require, exports, __document__, __dialog__, __createDatabaseCommand__, __collection__) {
    var document = __document__;
    var dialog = __dialog__;
    var createDatabaseCommand = __createDatabaseCommand__;
    var collection = __collection__;

    var copyDocuments = (function () {
        function copyDocuments(documents) {
            var _this = this;
            this.isCopyingDocs = ko.observable(true);
            this.documentsOrIdsText = ko.computed(function () {
                var prettifySpacing = 4;
                if (_this.isCopyingDocs()) {
                    return documents.map(function (d) {
                        return d.getId() + "\r\n" + JSON.stringify(d.toDto(false), null, prettifySpacing);
                    }).join("\r\n\r\n");
                } else {
                    return documents.map(function (d) {
                        return d.getId();
                    }).join(", ");
                }
            });
        }
        copyDocuments.prototype.attached = function () {
            var _this = this;
            this.selectText();

            jwerty.key("CTRL+C, enter", function (e) {
                e.preventDefault();
                _this.close();
            }, this, "#documentsText");
        };

        copyDocuments.prototype.deactivate = function () {
            $("#documentsText").unbind('keydown.jwerty');
        };

        copyDocuments.prototype.selectText = function () {
            $("#documentsText").select();
        };

        copyDocuments.prototype.close = function () {
            dialog.close(this);
        };

        copyDocuments.prototype.activateDocs = function () {
            this.isCopyingDocs(true);
            this.selectText();
        };

        copyDocuments.prototype.activateIds = function () {
            this.isCopyingDocs(false);
            this.selectText();
        };
        return copyDocuments;
    })();

    
    return copyDocuments;
});
//# sourceMappingURL=copyDocuments.js.map
