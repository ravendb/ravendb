/// <reference path="../../../../typings/tsd.d.ts"/>

import uploadAttachmentCommand = require("commands/database/documents/attachments/uploadAttachmentCommand");
import document = require("models/database/documents/document");
import database = require("models/resources/database");
import viewHelpers = require("common/helpers/view/viewHelpers")
import notificationCenter = require("common/notifications/notificationCenter");
import attachmentUpload = require("common/notifications/models/attachmentUpload");

class editDocumentUploader {

    static readonly filePickerSelector = "#uploadAttachmentFilePicker";

    private document: KnockoutObservable<document>;
    private db: KnockoutObservable<database>;
    private afterUpload: () => void;
    currentUpload = ko.observable<attachmentUpload>();
    
    uploadButtonText = ko.pureComputed(() => {
        if (this.currentUpload()) {
            return "Uploading (" + this.currentUpload().textualProgress() + ")";
        }
        return "Add Attachment";
    });

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
                viewHelpers.confirmationMessage(`Attachment '${file.name}' already exists.`, "Do you want to overwrite existing attachment?", {
                    buttons: ["No", "Yes, overwrite"]
                })
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

        const upload = attachmentUpload.forFile(this.db(), this.document().getId(), file.name);
        
        this.currentUpload(upload);
        
        notificationCenter.instance.monitorAttachmentUpload(upload);

        const command = new uploadAttachmentCommand(file, this.document().getId(), this.db(), event => upload.updateProgress(event));
        
        command
            .execute()
            .done(() => {
                this.afterUpload();
            })
            .fail(() => {
                // remove progress notification - failure notification will be shown instead.
                notificationCenter.instance.databaseNotifications.remove(upload);
            })
            .always(() => {
                this.spinners.upload(false);
                this.currentUpload(null);
            });
     
        upload.abort = () => command.abort(); 
    }
}

export = editDocumentUploader;
