import messagePublisher from "common/messagePublisher";
import { AccessPopover } from "components/common/AccessPopover";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import { ConditionalPopover } from "components/common/ConditionalPopover";
import useConfirm from "components/common/ConfirmDialog";
import { CustomDropdownToggle } from "components/common/Dropdown";
import { accessManagerSelectors } from "components/common/shell/accessManagerSliceSelectors";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import {
    LazyTableSelection,
    LazyTableSelectionState,
} from "components/common/virtualTable/hooks/useLazyTableSelection";
import { useEventsCollector } from "components/hooks/useEventsCollector";
import { useServices } from "components/hooks/useServices";
import { CollectionDeletionCallbacks } from "components/pages/database/documents/documentsList/hooks/useCollectionRemovalRedirect";
import CopyDocumentsModal, {
    CopyDocumentsModalData,
} from "components/pages/database/documents/documentsList/partials/CopyDocumentsModal";
import { useAppSelector } from "components/store";
import document from "models/database/documents/document";
import { useRef, useState } from "react";
import { useAsyncCallback } from "react-async-hook";
import Button from "react-bootstrap/Button";
import ButtonGroup from "react-bootstrap/ButtonGroup";
import Dropdown from "react-bootstrap/Dropdown";
import DeleteDocumentsModal from "viewmodels/database/documents/DeleteDocumentsModal";

interface DocumentsSelectionActionsProps {
    collectionName: string | null;
    trackedCollectionName: string;
    collectionDocumentCount: number | undefined;
    selection: LazyTableSelection<document>;
    collectionDeletionCallbacks: CollectionDeletionCallbacks;
    onSelectionDeleted: () => void;
}

const COPY_LIMIT = 100;
const ACTION_BUTTON_CLASS_NAME = "text-reset text-decoration-none";

export default function DocumentsSelectionActions({
    collectionName,
    trackedCollectionName,
    collectionDocumentCount,
    selection,
    collectionDeletionCallbacks,
    onSelectionDeleted,
}: DocumentsSelectionActionsProps) {
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const hasDatabaseWriteAccess = useAppSelector(accessManagerSelectors.getHasDatabaseWriteAccess)();
    const { databasesService } = useServices();
    const { reportEvent } = useEventsCollector();
    const confirm = useConfirm();

    const [isDeleteModalOpen, setIsDeleteModalOpen] = useState(false);
    const [isExclusiveDeleteRunning, setIsExclusiveDeleteRunning] = useState(false);
    const [copyModalData, setCopyModalData] = useState<CopyDocumentsModalData>(null);

    const isAllDocuments = collectionName === null;
    const { state, selectedCount, clear } = selection;

    const latestStateRef = useRef(state);
    latestStateRef.current = state;

    if (selectedCount === 0 && isDeleteModalOpen) {
        setIsDeleteModalOpen(false);
    }

    const asyncDeleteSelectedIds = useAsyncCallback(async (ids: string[]) => {
        const description = ids.length === 1 ? ids[0] : `${ids.length} documents`;

        try {
            await databasesService.deleteDocuments(ids, databaseName);
            messagePublisher.reportSuccess(`Deleted ${description}`);
        } catch (response) {
            messagePublisher.reportError(`Failed to delete ${description}`, response.responseText, response.statusText);
        } finally {
            onSelectionDeleted();
        }
    });

    const handleDelete = async () => {
        if (state.mode === "exclusive") {
            setIsDeleteModalOpen(true);
            return;
        }

        const isConfirmed = await confirm({
            title: `Delete ${state.selectedIds.length === 1 ? "document" : `${state.selectedIds.length} documents`}?`,
            message: (
                <ul className="overflow-auto m-0" style={{ maxHeight: "300px" }}>
                    {state.selectedIds.map((id) => (
                        <li key={id}>{id}</li>
                    ))}
                </ul>
            ),
            icon: "trash",
            actionColor: "danger",
            confirmText: "Delete",
        });

        if (isConfirmed) {
            await asyncDeleteSelectedIds.execute(state.selectedIds);
        }
    };

    const getSelectedIds = async (): Promise<string[]> => {
        if (state.mode === "inclusive") {
            return state.selectedIds;
        }

        const preview = await databasesService.getDocumentsPreview(
            databaseName,
            0,
            COPY_LIMIT + state.excludedIds.length,
            collectionName ?? undefined
        );

        const excludedIds = new Set(state.excludedIds);

        return preview.items
            .map((x) => x.getId())
            .filter((id) => !excludedIds.has(id))
            .slice(0, COPY_LIMIT);
    };

    const reportCopyError = (response: JQueryXHR) => {
        messagePublisher.reportError("Failed to copy the documents", response.responseText, response.statusText);
    };

    const showCopyModalIfSelectionUnchanged = (
        requestedState: LazyTableSelectionState,
        modalData: CopyDocumentsModalData
    ) => {
        if (latestStateRef.current === requestedState) {
            setCopyModalData(modalData);
        }
    };

    const asyncCopyDocuments = useAsyncCallback(async () => {
        reportEvent("documents", "copy");

        const requestedState = state;

        try {
            const ids = await getSelectedIds();
            const documents = await databasesService.getDocumentsWithMetadata(ids, databaseName);

            const missingIds = ids.filter((_, index) => !documents[index]);
            if (missingIds.length > 0) {
                messagePublisher.reportWarning(`Documents no longer exist: ${missingIds.join(", ")}`);
            }

            const existingDocuments = documents.filter((x) => x);
            if (existingDocuments.length === 0) {
                return;
            }

            showCopyModalIfSelectionUnchanged(requestedState, {
                title: "Documents",
                text: existingDocuments
                    .map((x) => x.getId() + "\r\n" + JSON.stringify(x.toDto(false), null, 4))
                    .join("\r\n\r\n"),
            });
        } catch (response) {
            reportCopyError(response);
        }
    });

    const asyncCopyIds = useAsyncCallback(async () => {
        reportEvent("documents", "copy-ids");

        const requestedState = state;

        try {
            const ids = await getSelectedIds();

            showCopyModalIfSelectionUnchanged(requestedState, {
                title: "Document IDs",
                text: ids.map((id) => `"${id}"`).join(", \r\n"),
            });
        } catch (response) {
            reportCopyError(response);
        }
    });

    const isCopyLimitExceeded = selectedCount > COPY_LIMIT;
    const isCopyDisabled = isCopyLimitExceeded || asyncCopyDocuments.loading || asyncCopyIds.loading;

    return (
        <>
            {selectedCount > 0 && (
                <div className="floating-bar text-nowrap" data-testid="selection-actions">
                    <span className="px-1">
                        <strong>{selectedCount.toLocaleString()}</strong> selected
                    </span>
                    <div className="vr" />
                    <ConditionalPopover
                        conditions={{
                            isActive: isCopyLimitExceeded,
                            message: `You can only copy up to ${COPY_LIMIT} documents`,
                        }}
                    >
                        <Dropdown as={ButtonGroup}>
                            <ButtonWithSpinner
                                variant="link"
                                className={ACTION_BUTTON_CLASS_NAME}
                                icon="copy"
                                onClick={asyncCopyDocuments.execute}
                                isSpinning={asyncCopyDocuments.loading}
                                disabled={isCopyDisabled}
                            >
                                Copy
                            </ButtonWithSpinner>
                            <div className="vr my-1" />
                            <Dropdown.Toggle
                                variant="link"
                                as={CustomDropdownToggle}
                                className={ACTION_BUTTON_CLASS_NAME}
                                disabled={isCopyDisabled}
                                title="More copy options"
                            />
                            <Dropdown.Menu>
                                <Dropdown.Item onClick={asyncCopyIds.execute}>Copy IDs</Dropdown.Item>
                            </Dropdown.Menu>
                        </Dropdown>
                    </ConditionalPopover>
                    <div className="vr" />
                    <AccessPopover accessRequired="DatabaseReadWrite">
                        <ButtonWithSpinner
                            variant="link"
                            className={ACTION_BUTTON_CLASS_NAME}
                            icon="trash"
                            onClick={handleDelete}
                            isSpinning={asyncDeleteSelectedIds.loading || isExclusiveDeleteRunning}
                            disabled={!hasDatabaseWriteAccess || isExclusiveDeleteRunning}
                        >
                            Delete
                        </ButtonWithSpinner>
                    </AccessPopover>
                    <div className="vr" />
                    <Button variant="link" className={ACTION_BUTTON_CLASS_NAME} onClick={clear}>
                        Clear selection
                    </Button>
                </div>
            )}

            {isDeleteModalOpen && state.mode === "exclusive" && (
                <DeleteDocumentsModal
                    close={() => setIsDeleteModalOpen(false)}
                    collectionName={trackedCollectionName}
                    collectionDocumentCount={collectionDocumentCount ?? selectedCount}
                    isAllDocuments={isAllDocuments}
                    excludedIds={state.excludedIds}
                    selectedCount={selectedCount}
                    onDeleteStarted={() => setIsExclusiveDeleteRunning(true)}
                    onDeleteCompleted={() => {
                        setIsExclusiveDeleteRunning(false);
                        onSelectionDeleted();
                    }}
                    {...collectionDeletionCallbacks}
                />
            )}
            {copyModalData && <CopyDocumentsModal {...copyModalData} close={() => setCopyModalData(null)} />}
        </>
    );
}
