import Modal from "components/common/Modal";
import React, { useState } from "react";
import Button from "react-bootstrap/Button";
import { useAsyncCallback } from "react-async-hook";
import { Icon } from "components/common/Icon";
import FileUploadPanel from "components/common/FileUploadPanel";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import useAttachmentUpload from "components/hooks/useAttachmentUpload";
import messagePublisher from "common/messagePublisher";
import document from "models/database/documents/document";
import database from "models/resources/database";
import pluralizeHelpers = require("common/helpers/text/pluralizeHelpers");

type AddAttachmentModalProps = {
    document: KnockoutObservable<document>;
    db: database;
    onUploaded: () => void;
    onClose: () => void;
};

export default function AddAttachmentModal({ document, db, onUploaded, onClose }: AddAttachmentModalProps) {
    const [selectedFiles, setSelectedFiles] = useState<File[]>([]);
    const { uploadItems, batchProgress, upload, abortCurrent } = useAttachmentUpload(document, db, onUploaded);

    const asyncUpload = useAsyncCallback(async () => {
        if (await upload(selectedFiles)) {
            onClose();
        }
    });

    const addFiles = (files: File[]) => {
        if (files.some((file) => !file.name.trim())) {
            messagePublisher.reportError("Failed to load file");
            return;
        }

        setSelectedFiles((current) => [
            ...current.filter((x) => !files.some((file) => file.name === x.name)),
            ...files,
        ]);
    };

    const removeFile = (file: File) => setSelectedFiles((current) => current.filter((x) => x !== file));

    const attachmentsLabel = pluralizeHelpers.pluralize(selectedFiles.length || 1, "attachment", "attachments", true);

    return (
        <Modal size="lg" show contentClassName="modal-border bulge-primary">
            <Modal.Header className="pb-0" onCloseClick={onClose}>
                <h3>
                    <Icon icon="attachment" color="primary" />
                    Add {attachmentsLabel}
                </h3>
            </Modal.Header>
            <Modal.Body>
                <FileUploadPanel
                    items={uploadItems(selectedFiles)}
                    onChange={addFiles}
                    onRemove={removeFile}
                    onCancel={abortCurrent}
                    onClearAll={() => setSelectedFiles([])}
                />
            </Modal.Body>
            <Modal.Footer>
                <Button variant="link" className="text-muted" onClick={onClose}>
                    Close
                </Button>
                <ButtonWithSpinner
                    className="rounded-pill"
                    variant="primary"
                    isSpinning={asyncUpload.loading}
                    onClick={asyncUpload.execute}
                    disabled={selectedFiles.length === 0}
                >
                    {batchProgress
                        ? `Uploading ${batchProgress.position}/${batchProgress.count}`
                        : `Upload ${attachmentsLabel}`}
                </ButtonWithSpinner>
            </Modal.Footer>
        </Modal>
    );
}
