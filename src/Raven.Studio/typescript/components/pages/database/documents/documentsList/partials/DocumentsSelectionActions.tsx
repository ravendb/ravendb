import messagePublisher from "common/messagePublisher";
import { AccessPopover } from "components/common/AccessPopover";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import { ConditionalPopover } from "components/common/ConditionalPopover";
import useConfirm from "components/common/ConfirmDialog";
import { CustomDropdownToggle } from "components/common/Dropdown";
import { accessManagerSelectors } from "components/common/shell/accessManagerSliceSelectors";
import { systemCollectionNames } from "components/common/shell/collectionsTrackerSlice";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { useEventsCollector } from "components/hooks/useEventsCollector";
import { useServices } from "components/hooks/useServices";
import { CollectionDeletionCallbacks } from "components/pages/database/documents/documentsList/hooks/useCollectionRemovalRedirect";
import { DocumentsSelection } from "components/pages/database/documents/documentsList/hooks/useDocumentsSelection";
import CopyDocumentsModal, {
    CopyDocumentsModalData,
} from "components/pages/database/documents/documentsList/partials/CopyDocumentsModal";
import { useAppSelector } from "components/store";
import { useState } from "react";
import { useAsyncCallback } from "react-async-hook";
import Button from "react-bootstrap/Button";
import ButtonGroup from "react-bootstrap/ButtonGroup";
import Dropdown from "react-bootstrap/Dropdown";
import DeleteDocumentsModal from "viewmodels/database/documents/DeleteDocumentsModal";

interface DocumentsSelectionActionsProps {
    // null means all documents
    collectionName: string | null;
    collectionDocumentCount: number | null;
    selection: DocumentsSelection;
    collectionDeletionCallbacks: CollectionDeletionCallbacks;
    onSelectionDeleted: () => void;
}

const copyLimit = 100;
const actionButtonClassName = "text-reset text-decoration-none";

// floating bar with the actions for the selected documents, rendered at the bottom of the table
export default function DocumentsSelectionActions({
    collectionName,
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
    const [copyModalData, setCopyModalData] = useState<CopyDocumentsModalData>(null);

    const isAllDocuments = collectionName === null;
    const { state, selectedCount, clear } = selection;

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
            copyLimit + state.excludedIds.length,
            collectionName ?? undefined
        );

        return preview.items
            .map((x) => x.getId())
            .filter((id) => !state.excludedIds.includes(id))
            .slice(0, copyLimit);
    };

    const asyncCopyDocuments = useAsyncCallback(async () => {
        reportEvent("documents", "copy");

        // the preview may contain incomplete values, so the documents are fetched again
        const ids = await getSelectedIds();
        const documents = await databasesService.getDocumentsWithMetadata(ids, databaseName);

        setCopyModalData({
            title: "Documents",
            text: documents.map((x) => x.getId() + "\r\n" + JSON.stringify(x.toDto(false), null, 4)).join("\r\n\r\n"),
        });
    });

    const asyncCopyIds = useAsyncCallback(async () => {
        reportEvent("documents", "copy-ids");

        const ids = await getSelectedIds();

        setCopyModalData({
            title: "Document IDs",
            text: ids.map((id) => `"${id}"`).join(", \r\n"),
        });
    });

    const isCopyLimitExceeded = selectedCount > copyLimit;
    const isCopyDisabled = isCopyLimitExceeded || asyncCopyDocuments.loading || asyncCopyIds.loading;

    if (selectedCount === 0) {
        return null;
    }

    return (
        <>
            <div className="floating-bar text-nowrap" data-testid="selection-actions">
                <span className="px-1">
                    <strong>{selectedCount.toLocaleString()}</strong> selected
                </span>
                <div className="vr" />
                <ConditionalPopover
                    conditions={{
                        isActive: isCopyLimitExceeded,
                        message: `You can only copy up to ${copyLimit} documents`,
                    }}
                >
                    <Dropdown as={ButtonGroup}>
                        <ButtonWithSpinner
                            variant="link"
                            className={actionButtonClassName}
                            icon="copy"
                            onClick={asyncCopyDocuments.execute}
                            isSpinning={asyncCopyDocuments.loading}
                            disabled={isCopyDisabled}
                        >
                            Copy
                        </ButtonWithSpinner>
                        <Dropdown.Toggle
                            variant="link"
                            as={CustomDropdownToggle}
                            className={actionButtonClassName}
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
                        className={actionButtonClassName}
                        icon="trash"
                        onClick={handleDelete}
                        isSpinning={asyncDeleteSelectedIds.loading}
                        disabled={!hasDatabaseWriteAccess}
                    >
                        Delete
                    </ButtonWithSpinner>
                </AccessPopover>
                <div className="vr" />
                <Button variant="link" className={actionButtonClassName} onClick={clear}>
                    Clear selection
                </Button>
            </div>

            {isDeleteModalOpen && state.mode === "exclusive" && (
                <DeleteDocumentsModal
                    close={() => setIsDeleteModalOpen(false)}
                    collectionName={collectionName ?? systemCollectionNames.allDocuments}
                    collectionDocumentCount={collectionDocumentCount ?? selectedCount}
                    isAllDocuments={isAllDocuments}
                    excludedIds={state.excludedIds}
                    selectedCount={selectedCount}
                    onDeleteCompleted={onSelectionDeleted}
                    {...collectionDeletionCallbacks}
                />
            )}
            {copyModalData && <CopyDocumentsModal {...copyModalData} close={() => setCopyModalData(null)} />}
        </>
    );
}
