import Modal from "components/common/Modal";
import { Icon } from "components/common/Icon";
import React, { useState } from "react";
import Form from "react-bootstrap/Form";
import Button from "react-bootstrap/Button";
import { useAsync } from "react-async-hook";
import { useServices } from "hooks/useServices";
import { useAppSelector } from "components/store";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import messagePublisher from "common/messagePublisher";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import notificationCenter from "common/notifications/notificationCenter";
import pluralizeHelpers from "common/helpers/text/pluralizeHelpers";
import genUtils from "common/generalUtils";
import studioSettings from "common/settings/studioSettings";
import { Switch } from "components/common/Checkbox";
import useBoolean from "hooks/useBoolean";
import { useIsMounted } from "components/hooks/useIsMounted";

interface DeleteDocumentsModalProps {
    close: () => void;
    collectionName: string;
    collectionDocumentCount: number;
    isAllDocuments: boolean;
    excludedIds: string[];
    selectedCount: number;
    onDeleteCompleted: () => void;
    onCollectionDeletionStarted: (collectionName: string) => void;
    onCollectionDeletionFailed: (collectionName: string) => void;
    onEntireCollectionDeleted: (collectionName: string) => void;
}

export default function DeleteDocumentsModal({
    close,
    collectionName,
    collectionDocumentCount,
    isAllDocuments,
    excludedIds,
    selectedCount,
    onDeleteCompleted,
    onCollectionDeletionStarted,
    onCollectionDeletionFailed,
    onEntireCollectionDeleted,
}: DeleteDocumentsModalProps) {
    // Note: wrapped in function to avoid type error (JQueryPromise<globalSettings>)
    const asyncGlobalSettings = useAsync(async () => await studioSettings.default.globalSettings(), []);

    const isRequireTypedConfirm =
        asyncGlobalSettings.result?.isRequireTypedConfirmationToDeleteDocuments.getValue() ?? true;

    const isSelectedAll = selectedCount === collectionDocumentCount;
    const { confirmText, handleTextChange, isConfirmed } = useDeleteConfirmation(isRequireTypedConfirm);

    const deleteCollection = useDeleteCollection({
        close,
        collectionName,
        collectionDocumentCount,
        isAllDocuments,
        excludedIds,
        selectedCount,
        onDeleteCompleted,
        onCollectionDeletionStarted,
        onCollectionDeletionFailed,
        onEntireCollectionDeleted,
    });

    const onConfirm = () => {
        if (!isConfirmed) {
            return;
        }
        deleteCollection.execute();
    };

    const toggleIsRequireTypedConfirm = async () => {
        if (!asyncGlobalSettings.result) {
            messagePublisher.reportError("Failed to load studio global settings");
            return;
        }

        asyncGlobalSettings.result.isRequireTypedConfirmationToDeleteDocuments.setValue(!isRequireTypedConfirm);
        await asyncGlobalSettings.execute();
    };

    return (
        <Modal show contentClassName="modal-border bulge-danger">
            <Modal.Header closeButton className="vstack gap-4" onCloseClick={close}>
                <div className="text-center">
                    <Icon icon="trash" color="danger" className="fs-1" margin="m-0" />
                </div>
                <div className="text-center lead">Delete {isSelectedAll ? "all" : "selected"} documents?</div>
            </Modal.Header>
            <Modal.Body>
                <CollectionsInfo
                    collectionName={collectionName}
                    isAllDocuments={isAllDocuments}
                    selectedCount={selectedCount}
                    isSelectedAll={isSelectedAll}
                />
                {isRequireTypedConfirm && (
                    <Form.Group>
                        <Form.Label className="fw-bold">Type DELETE to confirm</Form.Label>
                        <Form.Control placeholder="DELETE" value={confirmText} onChange={handleTextChange} />
                    </Form.Group>
                )}
            </Modal.Body>
            <Modal.Footer className="hstack justify-content-between">
                <Switch selected={isRequireTypedConfirm} toggleSelection={toggleIsRequireTypedConfirm} color="primary">
                    Require typed confirmation
                </Switch>
                <div className="hstack gap-2 flex-grow-1 justify-content-end">
                    <Button variant="link" onClick={close} className="link-muted">
                        Cancel
                    </Button>
                    <ButtonWithSpinner
                        isSpinning={deleteCollection.loading}
                        variant="danger"
                        onClick={onConfirm}
                        className="rounded-pill"
                        disabled={!isConfirmed}
                    >
                        Delete
                    </ButtonWithSpinner>
                </div>
            </Modal.Footer>
        </Modal>
    );
}

function useDeleteConfirmation(isRequireTypedConfirm: boolean) {
    const [confirmText, setConfirmText] = useState("");

    const handleTextChange = (e: React.ChangeEvent<HTMLInputElement>) => {
        setConfirmText(e.target.value.trim());
    };

    return {
        confirmText,
        handleTextChange,
        isConfirmed: isRequireTypedConfirm ? confirmText === "DELETE" : true,
    };
}

function useDeleteCollection({
    close,
    collectionName,
    isAllDocuments,
    excludedIds,
    selectedCount: documentCount,
    onDeleteCompleted,
    onCollectionDeletionStarted,
    onCollectionDeletionFailed,
    onEntireCollectionDeleted,
}: DeleteDocumentsModalProps) {
    const { databasesService } = useServices();
    const dbName = useAppSelector(databaseSelectors.activeDatabaseName);
    const { value: isLoading, setValue: setIsLoading } = useBoolean(false);
    const isMounted = useIsMounted();

    const execute = () => {
        setIsLoading(true);
        const collectionNameForApi = isAllDocuments ? "@all_docs" : collectionName;
        const isCollectionRemoval = excludedIds.length === 0 && !isAllDocuments;

        // This is JQueryPromise. Use 'done' to prevent wrong order of execution in event loop
        databasesService
            .deleteCollection(collectionNameForApi, dbName, excludedIds)
            .done((result) => {
                const operationId = result.OperationId;

                if (isCollectionRemoval) {
                    onCollectionDeletionStarted(collectionName);
                }

                notificationCenter.instance.openDetailsForOperationById(dbName, operationId);

                notificationCenter.instance
                    .monitorOperation(dbName, operationId)
                    .done(() => {
                        if (excludedIds.length === 0) {
                            messagePublisher.reportSuccess(`Deleted collection ${collectionName}`);
                        } else {
                            messagePublisher.reportSuccess(
                                `Deleted ${pluralizeHelpers.pluralize(documentCount, "document", "documents")} from ${collectionName}`
                            );
                        }

                        if (excludedIds.length === 0) {
                            onEntireCollectionDeleted(collectionName);
                        }
                    })
                    .fail(() => {
                        if (isCollectionRemoval) {
                            onCollectionDeletionFailed(collectionName);
                        }
                    })
                    .always(() => {
                        onDeleteCompleted();
                    });

                close();
            })
            .always(() => {
                if (isMounted()) {
                    setIsLoading(false);
                }
            });
    };

    return {
        execute,
        loading: isLoading,
    };
}

interface CollectionsInfoProps {
    collectionName: string;
    isAllDocuments: boolean;
    selectedCount: number;
    isSelectedAll: boolean;
}

function CollectionsInfo({ collectionName, isAllDocuments, selectedCount, isSelectedAll }: CollectionsInfoProps) {
    const collectionDisplayName = isAllDocuments ? "all" : collectionName;

    return (
        <p>
            {isSelectedAll ? "All" : "Selected"} documents from{" "}
            {isAllDocuments ? (
                <>
                    <b className="text-uppercase">{collectionDisplayName}</b> collections
                </>
            ) : (
                <>
                    collection <b>{collectionDisplayName}</b>
                </>
            )}{" "}
            will be deleted ({genUtils.formatNumberToStringFixed(selectedCount, 0)} documents).
        </p>
    );
}
