import document = require("models/database/documents/document");
import dialog = require("plugins/dialog");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import copyToClipboard = require("common/copyToClipboard");

class copyDocuments extends dialogViewModelBase {

    isCopyingDocs = ko.observable(true);
    documentsOrIdsText: KnockoutComputed<string>;

    constructor(private documents: Array<document>, elementToFocusOnDismissal?: string) {
        super(elementToFocusOnDismissal);

        this.documentsOrIdsText = ko.computed(() => {
            var prettifySpacing = 4;
            if (this.isCopyingDocs()) {
                return documents.map(d => d.getId() + "\r\n" + JSON.stringify(d.toDto(false), null, prettifySpacing)).join("\r\n\r\n");
            } else {
                return documents.map(d => d.getId()).join(", ");
            }
        });
    }

    attached() {
        super.attached();
        this.registerResizing("copyDocumentsResize");
        this.selectText();

        jwerty.key("CTRL+C, enter", e => {
            e.preventDefault();
            this.close();
        }, this, "#documentsText");

    }

    deactivate() {
        $("#documentsText").unbind('keydown.jwerty');
    }

    copyToClipboard() {
        var message = "Document";
        if (this.isCopyingDocs() === false) {
            message += " ID";
        }

        if (this.documents.length === 1) {
            message += " was";
        } else {
            message += "s were";
        }
        message += " copied to clipboard!";

        copyToClipboard.copy($("#documentsText").val(), message);
        this.close();
    }

    selectText() {
        $("#documentsText").select();
    }

    close() {
        dialog.close(this);
    }

    detached() {
        super.detached();
        this.unregisterResizing("copyDocumentsResize");
    }

    activateDocs() {
        this.isCopyingDocs(true);
        this.selectText();
    }

    activateIds() {
        this.isCopyingDocs(false);
        this.selectText();
    }
}

export = copyDocuments; 
