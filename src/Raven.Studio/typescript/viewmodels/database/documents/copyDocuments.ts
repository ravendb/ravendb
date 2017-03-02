import document = require("models/database/documents/document");
import dialog = require("plugins/dialog");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import copyToClipboard = require("common/copyToClipboard");

class copyDocuments extends dialogViewModelBase {

    isCopyingDocs = ko.observable(true);
    documentsText: KnockoutComputed<string>;

    constructor(documents: Array<document>, elementToFocusOnDismissal?: string) {
        super(elementToFocusOnDismissal);

        this.documentsText = ko.pureComputed(() => {
            const prettifySpacing = 4;
            return documents.map(d => d.getId() + "\r\n" + JSON.stringify(d.toDto(false), null, prettifySpacing)).join("\r\n\r\n");
        });
    }

    copyTextToClipboard() {
        dialog.close(this);
        copyToClipboard.copy(this.documentsText(), "Documents were copied to clipboard.");
    }

    close() {
        dialog.close(this);
    }

}

export = copyDocuments; 
