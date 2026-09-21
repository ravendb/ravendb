import { useState } from "react";
import { FileUploadItem } from "components/common/FileUploadList";
import editDocumentUploader = require("viewmodels/database/documents/editDocumentUploader");
import document = require("models/database/documents/document");
import database = require("models/resources/database");
import RemoteAttachmentParameters = Raven.Client.Documents.Operations.Attachments.RemoteAttachmentParameters;

type FileUploadState = Omit<FileUploadItem, "file">;

interface UseAttachmentUploadResult {
    uploadItems: (files: File[]) => FileUploadItem[];
    batchProgress: attachmentUploadProgress;
    upload: (files: File[], remoteParameters?: RemoteAttachmentParameters) => Promise<boolean>;
    abortCurrent: () => void;
}

export default function useAttachmentUpload(
    document: KnockoutObservable<document>,
    db: database,
    onUploaded: () => void
): UseAttachmentUploadResult {
    const [uploader] = useState(() => new editDocumentUploader(document, db, onUploaded));
    const [batchProgress, setBatchProgress] = useState<attachmentUploadProgress>(null);
    const [uploadStates, setUploadStates] = useState<Record<string, FileUploadState>>({});

    const upload = async (files: File[], remoteParameters?: RemoteAttachmentParameters) => {
        const finalStatuses = new Map<string, attachmentUploadStatus>();
        setUploadStates({});

        const onProgress = (progress: attachmentUploadProgress) => {
            finalStatuses.set(progress.fileName, progress.status);
            setBatchProgress(progress);
            setUploadStates((states) => ({
                ...states,
                [progress.fileName]: { status: progress.status, loaded: progress.loaded, total: progress.total },
            }));
        };

        try {
            await uploader.uploadFiles(files, remoteParameters, onProgress);
        } finally {
            setBatchProgress(null);
        }

        return files.every((file) => finalStatuses.get(file.name) === "uploaded");
    };

    return {
        uploadItems: (files) => files.map((file) => ({ file, status: "selected", ...uploadStates[file.name] })),
        batchProgress,
        upload,
        abortCurrent: () => uploader.abortCurrent(),
    };
}
