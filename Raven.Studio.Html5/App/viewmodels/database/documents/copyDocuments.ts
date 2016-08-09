import document = require("models/database/documents/document");
import dialog = require("plugins/dialog");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import copyToClipboard = require("common/copyToClipboard");
import database = require("models/resources/database");
import getDocumentsByIdsCommand = require("commands/database/documents/getDocumentsByIdsCommand");

class copyDocuments extends dialogViewModelBase {

    activeDatabase = ko.observable<database>().subscribeTo("ActivateDatabase", true);

    isCopyingDocs = ko.observable(true);
    documentsOrIdsText: KnockoutComputed<string>;

    private documents: KnockoutObservableArray<document> = ko.observableArray<document>([]);

    constructor(documents: Array<document>, elementToFocusOnDismissal?: string) {
        super(elementToFocusOnDismissal);

        var ids = documents.map(x => x.getId()).filter(x => x);
        if (ids.length !== documents.length) {
            // all documents has empty ids - we have to use copy passed via constructor
            this.documents(documents);
        } else {
            // all passed documents has ids - query for those doc to avoid trimmed content generated via doc-preview endpoint
            new getDocumentsByIdsCommand(ids, this.activeDatabase()).execute()
                .done((results: Array<document>) => this.documents(results));
        }

        this.documentsOrIdsText = ko.computed(() => {
            var prettifySpacing = 4;
            if (this.isCopyingDocs()) {
                return this.documents().map(d => d.getId() + "\r\n" + JSON.stringify(d.toDto(false), null, prettifySpacing)).join("\r\n\r\n");
            } else {
                return this.documents().map(d => d.getId()).join(", ");
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
