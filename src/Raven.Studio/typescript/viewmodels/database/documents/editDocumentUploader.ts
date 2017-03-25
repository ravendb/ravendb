/// <reference path="../../../../typings/tsd.d.ts"/>

import viewHelpers = require("common/helpers/view/viewHelpers");
import uploadAttachmentCommand = require("commands/database/documents/attachments/uploadAttachmentCommand");
import document = require("models/database/documents/document");
import database = require("models/resources/database");

//TODO: for now we simple impl, which doesn't support drag-and-drop or simultaneous uploads
class editDocumentUploader {

    static readonly filePickerSelector = "#uploadAttachmentFilePicker";

    private document: KnockoutObservable<document>;
    private db: KnockoutObservable<database>;
    private afterUpload: () => void;

    fileName = ko.observable<string>();
    hasFileSelected = ko.observable(false);

    validationGroup: KnockoutValidationGroup = ko.validatedObservable({
        fileName: this.fileName
    });

    constructor(document: KnockoutObservable<document>, db: KnockoutObservable<database>, afterUpload: () => void) {
        this.document = document;
        this.db = db;
        this.afterUpload = afterUpload;

        this.fileName.extend({
            required: true
        });
    }

    fileSelected(fileName: string) {
        const isFileSelected = fileName ? !!fileName.trim() : false;
        this.hasFileSelected(isFileSelected);
        this.fileName(isFileSelected ? fileName.split(/(\\|\/)/g).pop() : null);
    }

    uploadFile() {
        if (viewHelpers.isValid(this.validationGroup)) {
            const selector = $(editDocumentUploader.filePickerSelector)[0] as HTMLInputElement;
            const file = selector.files[0];
            new uploadAttachmentCommand(file, this.document().getId(), this.db())
                .execute()
                .done(() => {
                    this.fileName("");
                    this.afterUpload();
                    this.fileName.clearError();
                });

        }
    }
}

export = editDocumentUploader;