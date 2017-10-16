/// <reference path="../../../../typings/tsd.d.ts"/>

import uploadAttachmentCommand = require("commands/database/documents/attachments/uploadAttachmentCommand");
import document = require("models/database/documents/document");
import database = require("models/resources/database");
import viewHelpers = require("common/helpers/view/viewHelpers")

//TODO: for now we simple impl, which doesn't support drag-and-drop or simultaneous uploads
class editDocumentUploader {

    static readonly filePickerSelector = "#uploadAttachmentFilePicker";

    private document: KnockoutObservable<document>;
    private db: KnockoutObservable<database>;
    private afterUpload: () => void;

    spinners = {
        upload: ko.observable<boolean>(false)
    };

    constructor(document: KnockoutObservable<document>, db: KnockoutObservable<database>, afterUpload: () => void) {
        this.document = document;
        this.db = db;
        this.afterUpload = afterUpload;
    }

    fileSelected(fileName: string) {
        if (fileName) {
            const selector = $(editDocumentUploader.filePickerSelector)[0] as HTMLInputElement;
            const file = selector.files[0];

            const nameAlreadyExists = this.findAttachment(file.name);
            if (nameAlreadyExists) {
                viewHelpers.confirmationMessage(`Attachment '${file.name}' already exists.`, "Do you want to overwrite existing attachment?", ["No", "Yes, overwrite"])
                    .done(result => {
                        if (result.can) {
                            this.uploadInternal(file);
                        }
                    });
            } else {
                this.uploadInternal(file);
            }
        }
    }

    private findAttachment(name: string) {
        const attachments = this.document().__metadata.attachments();
        if (!attachments) {
            return null;
        }

        return attachments.find(x => x.Name === name);
    }

    private uploadInternal(file: File) {
        this.spinners.upload(true);

        new uploadAttachmentCommand(file, this.document().getId(), this.db())
            .execute()
            .done(() => {
                this.afterUpload();
            })
            .always(() => this.spinners.upload(false));
    }
}

export = editDocumentUploader;
