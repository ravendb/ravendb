import Modal from "components/common/Modal";
import { Icon } from "components/common/Icon";
import React, { useState } from "react";
import Form from "react-bootstrap/Form";
import Button from "react-bootstrap/Button";
import { useAsync, useAsyncCallback, UseAsyncReturn } from "react-async-hook";
import { useServices } from "hooks/useServices";
import { useAppSelector } from "components/store";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { LazyLoad } from "components/common/LazyLoad";
import collectionsStats from "models/database/documents/collectionsStats";
import virtualGridController from "widgets/virtualGrid/virtualGridController";
import document from "models/database/documents/document";
import virtualGridSelection from "widgets/virtualGrid/virtualGridSelection";
import messagePublisher from "common/messagePublisher";
import collection from "models/database/documents/collection";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import notificationCenter from "common/notifications/notificationCenter";
import collectionsTracker from "common/helpers/database/collectionsTracker";
import pluralizeHelpers from "common/helpers/text/pluralizeHelpers";
import genUtils from "common/generalUtils";

interface DeleteDocumentsModalProps {
    close: () => void;
    gridController: virtualGridController<document>;
    onDeleteCompleted?: () => void;
    currentCollection: KnockoutObservable<collection>;
}

export default function DeleteDocumentsModal({
    close,
    gridController,
    onDeleteCompleted,
    currentCollection,
}: DeleteDocumentsModalProps) {
    const selection = gridController.selection();

    const dbName = useAppSelector(databaseSelectors.activeDatabaseName);

    const { confirmText, handleTextChange, isConfirmed } = useDeleteConfirmation();
    const { tasksService } = useServices();
    const collectionsList = useAsync(() => tasksService.fetchCollectionsStats(dbName), []);

    const deleteCollection = useDeleteCollection(
        currentCollection,
        selection.excluded.map((doc) => doc.getId()),
        selection.count,
        close,
        onDeleteCompleted
    );

    const onConfirm = async () => {
        if (!isConfirmed) {
            return;
        }
        await deleteCollection.execute();
    };

    return (
        <Modal show contentClassName="modal-border bulge-danger">
            <Modal.Header closeButton className="vstack gap-4" onCloseClick={close}>
                <div className="text-center">
                    <Icon icon="trash" color="danger" className="fs-1" margin="m-0" />
                </div>
                <div className="text-center lead">Delete all documents?</div>
            </Modal.Header>
            <Modal.Body>
                <CollectionsInfo
                    collectionsList={collectionsList}
                    virtualGridSelection={selection}
                    currentCollection={currentCollection}
                />
                <Form.Group>
                    <Form.Label className="fw-bold">Type DELETE to confirm</Form.Label>
                    <Form.Control placeholder="DELETE" value={confirmText} onChange={handleTextChange} />
                </Form.Group>
            </Modal.Body>
            <Modal.Footer>
                <Button variant="link" onClick={close} className="link-muted">
                    Cancel
                </Button>
                <ButtonWithSpinner
                    isSpinning={deleteCollection.loading}
                    variant="danger"
                    onClick={onConfirm}
                    className="rounded-pill"
                    disabled={!isConfirmed || collectionsList.loading}
                >
                    Delete
                </ButtonWithSpinner>
            </Modal.Footer>
        </Modal>
    );
}

function useDeleteConfirmation() {
    const [confirmText, setConfirmText] = useState("");

    const handleTextChange = (e: React.ChangeEvent<HTMLInputElement>) => {
        setConfirmText(e.target.value.trim());
    };

    return {
        confirmText,
        handleTextChange,
        isConfirmed: confirmText === "DELETE",
    };
}

function useDeleteCollection(
    currentCollection: KnockoutObservable<collection>,
    excludedIds: string[],
    documentCount: number,
    close: () => void,
    onDeleteCompleted: () => void
) {
    const { databasesService } = useServices();
    const dbName = useAppSelector(databaseSelectors.activeDatabaseName);

    const deleteCollectionAsync = useAsyncCallback(
        async () => {
            const collectionNameForApi = currentCollection().isAllDocuments ? "@all_docs" : currentCollection().name;
            const result = await databasesService.deleteCollection(collectionNameForApi, dbName, excludedIds);
            return result.OperationId;
        },
        {
            onError: (error) => messagePublisher.reportError("Failed to delete collection", error.message),
        }
    );

    const monitorOperationAsync = useAsyncCallback(
        async (operationId: number) => {
            notificationCenter.instance.openDetailsForOperationById(dbName, operationId);
            close();
            await notificationCenter.instance.databaseOperationsWatch
                .monitorOperation(operationId)
                .done(() => {
                    if (excludedIds.length === 0) {
                        messagePublisher.reportSuccess(`Deleted collection ${currentCollection().name}`);
                    } else {
                        messagePublisher.reportSuccess(
                            `Deleted ${pluralizeHelpers.pluralize(documentCount, "document", "documents")} from ${currentCollection().name}`
                        );
                    }

                    if (excludedIds.length === 0) {
                        // if entire collection was deleted then go to 'all documents'
                        const allDocsCollection = collectionsTracker.default.getAllDocumentsCollection();
                        if (currentCollection() !== allDocsCollection) {
                            currentCollection(allDocsCollection);
                        }
                    }
                })
                .always(() => onDeleteCompleted());
        },
        {
            onError: (error) => messagePublisher.reportError("Failed to monitor delete operation", error.message),
        }
    );

    const execute = async () => {
        const operationId = await deleteCollectionAsync.execute();
        if (operationId) {
            await monitorOperationAsync.execute(operationId);
        }
    };

    return {
        execute,
        loading: deleteCollectionAsync.loading || monitorOperationAsync.loading,
    };
}

interface CollectionsInfoProps {
    virtualGridSelection: virtualGridSelection<document>;
    collectionsList: UseAsyncReturn<collectionsStats>;
    currentCollection: KnockoutObservable<collection>;
}

function CollectionsInfo({ virtualGridSelection, collectionsList, currentCollection }: CollectionsInfoProps) {
    const collectionName =
        currentCollection().name === collection.allDocumentsCollectionName ? "all" : currentCollection().name;
    const isAllDocuments = collectionName === "all";

    return (
        <LazyLoad active={collectionsList.loading}>
            <p>
                All documents from{" "}
                {isAllDocuments ? (
                    <>
                        <b className="text-uppercase">{collectionName}</b> collections
                    </>
                ) : (
                    <>
                        collection <b>{collectionName}</b>
                    </>
                )}{" "}
                will be deleted ({genUtils.formatNumberToStringFixed(virtualGridSelection.count, 0)} documents).
            </p>
        </LazyLoad>
    );
}
