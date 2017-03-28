/// <reference path="../../../../typings/tsd.d.ts"/>

import uploadAttachmentCommand = require("commands/database/documents/attachments/uploadAttachmentCommand");
import document = require("models/database/documents/document");
import database = require("models/resources/database");

//TODO: for now we simple impl, which doesn't support drag-and-drop or simultaneous uploads
class editDocumentUploader {

    static readonly filePickerSelector = "#uploadAttachmentFilePicker";

    private document: KnockoutObservable<document>;
    private db: KnockoutObservable<database>;
    private afterUpload: () => void;

    constructor(document: KnockoutObservable<document>, db: KnockoutObservable<database>, afterUpload: () => void) {
        this.document = document;
        this.db = db;
        this.afterUpload = afterUpload;
    }

    fileSelected(fileName: string) {
        if (fileName) {
            const selector = $(editDocumentUploader.filePickerSelector)[0] as HTMLInputElement;
            const file = selector.files[0];
            new uploadAttachmentCommand(file, this.document().getId(), this.db())
                .execute()
                .done(() => {
                    this.afterUpload();
                });
        }
    }
}

export = editDocumentUploader;