import document = require("models/database/documents/document");
import dialog = require("plugins/dialog");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import copyToClipboard = require("common/copyToClipboard");

class copyDocumentIds extends dialogViewModelBase {

    idsText: KnockoutComputed<string>;

    constructor(documents: Array<document>, elementToFocusOnDismissal?: string) {
        super(elementToFocusOnDismissal);

        this.idsText = ko.pureComputed(() => documents.map(d => d.getId()).join(", "));
    }

    copyTextToClipboard() {
        dialog.close(this);
        copyToClipboard.copy(this.idsText(), "Document ids were copied to clipboard.");
    }

    close() {
        dialog.close(this);
    }

   
}

export = copyDocumentIds; 
