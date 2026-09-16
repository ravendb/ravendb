/// <reference path="../../../../typings/tsd.d.ts"/>

import uploadAttachmentCommand = require("commands/database/documents/attachments/uploadAttachmentCommand");
import document = require("models/database/documents/document");
import database = require("models/resources/database");
import viewHelpers = require("common/helpers/view/viewHelpers")
import notificationCenter = require("common/notifications/notificationCenter");
import attachmentUpload = require("common/notifications/models/attachmentUpload");
import pluralizeHelpers = require("common/helpers/text/pluralizeHelpers");
import RemoteAttachmentParameters = Raven.Client.Documents.Operations.Attachments.RemoteAttachmentParameters;

type progressCallback = (progress: attachmentUploadProgress) => void;

class editDocumentUploader {

    static readonly filePickerSelector = "#uploadAttachmentFilePicker";

    currentUpload = ko.observable<attachmentUpload>();
    private batchPosition = ko.observable<number>(0);
    private batchSize = ko.observable<number>(0);

    uploadButtonText = ko.pureComputed(() => {
        const upload = this.currentUpload();
        if (upload) {
            return `Uploading ${this.batchPosition()}/${this.batchSize()} (${upload.textualProgress()})`;
        }
        return "Add Attachment";
    });

    spinners = {
        upload: ko.observable<boolean>(false)
    };

    constructor(private document: KnockoutObservable<document>, private db: database, private afterUpload: () => void) {}

    async uploadFiles(files: File[], remoteParameters?: RemoteAttachmentParameters, onProgress?: progressCallback) {
        const filesToUpload = await this.resolveNameConflicts(files);
        const skipped = files.filter(x => !filesToUpload.includes(x));

        this.batchPosition(0);
        this.batchSize(filesToUpload.length);
        skipped.forEach(x => this.report(onProgress, x, "skipped", 0, x.size));

        if (!filesToUpload.length) {
            return;
        }

        this.spinners.upload(true);

        for (let i = 0; i < filesToUpload.length; i++) {
            this.batchPosition(i + 1);
            await this.uploadSingle(filesToUpload[i], remoteParameters, onProgress);
        }

        $(editDocumentUploader.filePickerSelector).val("");
        this.spinners.upload(false);
        this.afterUpload();
    }

    abortCurrent() {
        this.currentUpload()?.abortUpload();
    }

    private async resolveNameConflicts(files: File[]): Promise<File[]> {
        const existingNames = new Set((this.document().__metadata.attachments() ?? []).map(x => x.Name));
        const conflicting = files.filter(x => existingNames.has(x.name));
        if (!conflicting.length) {
            return files;
        }

        const overwrite = await this.confirmOverwrite(conflicting);
        return overwrite ? files : files.filter(x => !existingNames.has(x.name));
    }

    private confirmOverwrite(conflicting: File[]): Promise<boolean> {
        const title = `Overwrite existing ${pluralizeHelpers.pluralize(conflicting.length, "attachment", "attachments", true)}?`;
        const names = conflicting.map(x => `'${x.name}'`).join(", ");
        const message = `Document already has: ${names}. Overwriting replaces current content, skipping uploads only the new files.`;

        return new Promise<boolean>(resolve => {
            viewHelpers.confirmationMessage(title, message, {
                buttons: ["No, skip existing", "Yes, overwrite"],
                forceRejectWithResolve: true
            })
                .done(result => resolve(result.can))
                .fail(() => resolve(false));
        });
    }

    private async uploadSingle(file: File, remoteParameters?: RemoteAttachmentParameters, onProgress?: progressCallback) {
        const documentId = this.document().getId();
        const upload = attachmentUpload.forFile(this.db, documentId, file.name);
        this.currentUpload(upload);
        notificationCenter.instance.monitorAttachmentUpload(upload);

        this.report(onProgress, file, "uploading", 0, file.size);

        const command = new uploadAttachmentCommand(file, documentId, this.db, event => {
            upload.updateProgress(event);
            if (event.lengthComputable) {
                this.report(onProgress, file, "uploading", event.loaded, event.total);
            }
        }, remoteParameters);

        let cancelled = false;
        upload.abort = () => {
            cancelled = true;
            command.abort();
        };

        try {
            await command.execute();
            this.report(onProgress, file, "uploaded", file.size, file.size);
        } catch {
            notificationCenter.instance.databaseNotifications.remove(upload);
            this.report(onProgress, file, cancelled ? "cancelled" : "failed", 0, file.size);
        } finally {
            this.currentUpload(null);
        }
    }

    private report(onProgress: progressCallback | undefined, file: File, status: attachmentUploadStatus, loaded: number, total: number) {
        onProgress?.({
            position: this.batchPosition(),
            count: this.batchSize(),
            fileName: file.name,
            status,
            loaded,
            total
        });
    }
}

export = editDocumentUploader;
