import { waitFor } from "test/rtlTestUtils";
import { DatabasesStubs } from "test/stubs/DatabasesStubs";
import document = require("models/database/documents/document");
import uploadAttachmentCommand = require("commands/database/documents/attachments/uploadAttachmentCommand");
import viewHelpers = require("common/helpers/view/viewHelpers");
import notificationCenter = require("common/notifications/notificationCenter");
import attachmentUpload = require("common/notifications/models/attachmentUpload");
import editDocumentUploader = require("viewmodels/database/documents/editDocumentUploader");
import AttachmentDetails = Raven.Client.Documents.Operations.Attachments.AttachmentDetails;
import RemoteAttachmentParameters = Raven.Client.Documents.Operations.Attachments.RemoteAttachmentParameters;

interface ExecutedCommand {
    file: File;
    documentId: string;
    remoteParameters: RemoteAttachmentParameters;
}

function documentWithAttachments(...names: string[]) {
    const attachments = names.map((Name) => ({ Name, ContentType: "text/plain", Hash: "", Size: 1 }));
    return ko.observable(
        new document({ "@metadata": { "@id": "users/1", "@attachments": attachments } } as documentDto)
    );
}

function answerOverwrite(can: boolean) {
    return jest
        .spyOn(viewHelpers, "confirmationMessage")
        .mockReturnValue($.Deferred<confirmDialogResult>().resolve({ can }).promise());
}

function describeProgress(events: attachmentUploadProgress[]) {
    return events.map((x) => `${x.position}/${x.count} ${x.fileName} ${x.status}`);
}

describe("editDocumentUploader", () => {
    const db = DatabasesStubs.nonShardedSingleNodeDatabase();
    const remoteParameters: RemoteAttachmentParameters = { At: "2026-09-17T10:00:00.000Z", Identifier: "s3-main" };
    const fileA = new File(["aaaa"], "a.txt");
    const fileB = new File(["bb"], "b.txt");

    let execute: jest.SpyInstance;
    let executedCommands: ExecutedCommand[];
    let onProgress: jest.Mock<void, [attachmentUploadProgress]>;
    let afterUpload: jest.Mock;

    beforeEach(() => {
        executedCommands = [];
        onProgress = jest.fn();
        afterUpload = jest.fn();
        notificationCenter.instance.databaseNotifications.removeAll();
        execute = jest
            .spyOn(uploadAttachmentCommand.prototype, "execute")
            .mockImplementation(function (this: unknown) {
                executedCommands.push(this as ExecutedCommand);
                return $.Deferred<AttachmentDetails>().resolve().promise();
            });
    });

    afterEach(() => {
        jest.restoreAllMocks();
    });

    it("uploads files one after another and reports each step", async () => {
        const uploader = new editDocumentUploader(documentWithAttachments(), db, afterUpload);

        await uploader.uploadFiles([fileA, fileB], remoteParameters, onProgress);

        expect(describeProgress(onProgress.mock.calls.map(([x]) => x))).toEqual([
            "1/2 a.txt uploading",
            "1/2 a.txt uploaded",
            "2/2 b.txt uploading",
            "2/2 b.txt uploaded",
        ]);
        expect(executedCommands.map((x) => x.file.name)).toEqual(["a.txt", "b.txt"]);
        expect(executedCommands[0].documentId).toBe("users/1");
        expect(executedCommands[0].remoteParameters).toBe(remoteParameters);
        expect(afterUpload).toHaveBeenCalledTimes(1);
    });

    it("skips files that already exist when overwrite is declined", async () => {
        const confirmation = answerOverwrite(false);
        const uploader = new editDocumentUploader(documentWithAttachments("a.txt"), db, afterUpload);

        await uploader.uploadFiles([fileA, fileB], remoteParameters, onProgress);

        expect(confirmation).toHaveBeenCalledTimes(1);
        expect(confirmation.mock.calls[0][1]).toContain("'a.txt'");
        expect(describeProgress(onProgress.mock.calls.map(([x]) => x))).toEqual([
            "0/1 a.txt skipped",
            "1/1 b.txt uploading",
            "1/1 b.txt uploaded",
        ]);
        expect(executedCommands.map((x) => x.file.name)).toEqual(["b.txt"]);
        expect(afterUpload).toHaveBeenCalledTimes(1);
    });

    it("overwrites files that already exist when confirmed", async () => {
        answerOverwrite(true);
        const uploader = new editDocumentUploader(documentWithAttachments("a.txt", "b.txt"), db, afterUpload);

        await uploader.uploadFiles([fileA, fileB], remoteParameters, onProgress);

        expect(executedCommands.map((x) => x.file.name)).toEqual(["a.txt", "b.txt"]);
        expect(onProgress.mock.calls.map(([x]) => x.status)).not.toContain("skipped");
    });

    it("reports a failed upload, drops its notification and continues with the next file", async () => {
        execute.mockImplementationOnce(() => $.Deferred<AttachmentDetails>().reject().promise());
        const uploader = new editDocumentUploader(documentWithAttachments(), db, afterUpload);

        await uploader.uploadFiles([fileA, fileB], remoteParameters, onProgress);

        expect(describeProgress(onProgress.mock.calls.map(([x]) => x))).toEqual([
            "1/2 a.txt uploading",
            "1/2 a.txt failed",
            "2/2 b.txt uploading",
            "2/2 b.txt uploaded",
        ]);
        const notifications = notificationCenter.instance.databaseNotifications() as attachmentUpload[];
        expect(notifications.map((x) => x.fileName)).toEqual(["b.txt"]);
        expect(afterUpload).toHaveBeenCalledTimes(1);
    });

    it("reports the current upload as cancelled when aborted", async () => {
        const pendingUpload = $.Deferred<AttachmentDetails>();
        execute.mockImplementationOnce(() => pendingUpload.promise());
        const uploader = new editDocumentUploader(documentWithAttachments(), db, afterUpload);

        const batch = uploader.uploadFiles([fileA], remoteParameters, onProgress);
        await waitFor(() => expect(execute).toHaveBeenCalledTimes(1));
        uploader.abortCurrent();
        pendingUpload.reject();
        await batch;

        expect(onProgress.mock.calls.map(([x]) => x.status)).toEqual(["uploading", "cancelled"]);
    });
});
