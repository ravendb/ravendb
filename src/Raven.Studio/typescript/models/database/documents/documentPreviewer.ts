import app = require("durandal/app");
import document = require("models/database/documents/document");
import documentMetadata = require("models/database/documents/documentMetadata");
import showDataDialog = require("viewmodels/common/showDataDialog");
import database = require("models/resources/database");
import viewHelpers = require("common/helpers/view/viewHelpers");
import getDocumentWithMetadataCommand = require("commands/database/documents/getDocumentWithMetadataCommand");

class documentPreviewer {
    static preview(documentId: KnockoutObservable<string>, db: KnockoutObservable<database>, validationGroup: KnockoutValidationGroup, spinner?: KnockoutObservable<boolean>){
        if (spinner) {
            spinner(true);
        }
        viewHelpers.asyncValidationCompleted(validationGroup)
            .then(() => {
                if (viewHelpers.isValid(validationGroup)) {
                    new getDocumentWithMetadataCommand(documentId(), db())
                        .execute()
                        .done((doc: document) => {
                            const docDto = doc.toDto(true);
                            const metaDto = docDto["@metadata"];
                            documentMetadata.filterMetadata(metaDto);
                            const text = JSON.stringify(docDto, null, 4);
                            app.showBootstrapDialog(new showDataDialog("Document: " + doc.getId(), text, "javascript"));
                        })
                        .always(() => spinner(false));
                } else {
                    if (spinner) {
                        spinner(false);
                    }
                }
            });
    }
}

export = documentPreviewer;
