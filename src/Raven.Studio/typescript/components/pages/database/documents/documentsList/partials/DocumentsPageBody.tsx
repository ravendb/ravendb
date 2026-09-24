import { getCoreRowModel, useReactTable } from "@tanstack/react-table";
import { collectionsTrackerSelectors, systemCollectionNames } from "components/common/shell/collectionsTrackerSlice";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { useTableDisplaySettingsSheet } from "components/common/virtualTable/commonComponents/columnsSelect/TableDisplaySettings";
import { useLazyRows } from "components/common/virtualTable/hooks/useLazyRows";
import { useLazyTableSelection } from "components/common/virtualTable/hooks/useLazyTableSelection";
import LazyVirtualTable from "components/common/virtualTable/LazyVirtualTable";
import { lazyTableOptions } from "components/common/virtualTable/utils/lazyTableUtils";
import { virtualTableUtils } from "components/common/virtualTable/utils/virtualTableUtils";
import { useResizeObserver } from "components/hooks/useResizeObserver";
import { useServices } from "components/hooks/useServices";
import { useCollectionRemovalRedirect } from "components/pages/database/documents/documentsList/hooks/useCollectionRemovalRedirect";
import { useDocumentsColumns } from "components/pages/database/documents/documentsList/hooks/useDocumentsColumns";
import { useDocumentsDataChanged } from "components/pages/database/documents/documentsList/hooks/useDocumentsDataChanged";
import { useFullDocumentProvider } from "components/pages/database/documents/documentsList/hooks/useFullDocumentProvider";
import DocumentsListToolbar from "components/pages/database/documents/documentsList/partials/DocumentsListToolbar";
import DocumentsSelectionActions from "components/pages/database/documents/documentsList/partials/DocumentsSelectionActions";
import { useAppSelector } from "components/store";
import document from "models/database/documents/document";
import { useRef, useState } from "react";

interface DocumentsPageBodyProps {
    // null means all documents
    collectionName: string | null;
}

const getDocumentId = (doc: document) => doc.getId();

export default function DocumentsPageBody({ collectionName }: DocumentsPageBodyProps) {
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const isSharded = useAppSelector(databaseSelectors.activeDatabase)?.isSharded ?? false;
    const collectionDocumentCount =
        useAppSelector(
            collectionsTrackerSelectors.collectionByName(collectionName ?? systemCollectionNames.allDocuments)
        )?.documentCount ?? null;
    const { databasesService } = useServices();

    const { isDataChanged, trackResultEtag, reset: resetDataChanged } = useDocumentsDataChanged(collectionName);
    const fullDocumentProvider = useFullDocumentProvider(databaseName);
    const collectionDeletionCallbacks = useCollectionRemovalRedirect({ databaseName, collectionName });

    const bodyRef = useRef<HTMLDivElement>(null);
    const { width: bodyWidthInPx } = useResizeObserver({ ref: bodyRef });

    const columns = useDocumentsColumns({
        databaseName,
        collectionName,
        tableBodyWidthInPx: virtualTableUtils.getTableBodyWidth(bodyWidthInPx ?? 0),
        fullDocumentProvider,
    });

    const lazyRows = useLazyRows<document>({
        fetchMode: isSharded ? "continuationToken" : "skipTake",
        fetchData: async (skip, take, continuationToken) => {
            const result = await databasesService.getDocumentsPreview(
                databaseName,
                skip,
                take,
                collectionName ?? undefined,
                columns.previewBindings.length > 0 ? columns.previewBindings : undefined,
                columns.fullBindings.length > 0 ? columns.fullBindings : undefined,
                continuationToken
            );

            columns.onAvailableColumns(result.availableColumns);
            trackResultEtag(result.resultEtag);

            return result;
        },
        // the bindings decide which values the rows hold, so the table starts over when they change
        reloadDependencies: [columns.previewBindings, columns.fullBindings],
    });

    const [isPaginated, setIsPaginated] = useState(false);

    const selection = useLazyTableSelection({
        lazyRows,
        getId: getDocumentId,
        totalCount: collectionDocumentCount ?? lazyRows.totalCount,
        isPaginated,
    });

    const table = useReactTable({
        ...lazyTableOptions,
        data: lazyRows.data,
        columns: columns.columnDefs,
        getRowId: getDocumentId,
        meta: { lazySelection: selection },
        state: { rowSelection: selection.rowSelection, ...columns.tableState },
        onColumnVisibilityChange: columns.onColumnVisibilityChange,
        onColumnOrderChange: columns.onColumnOrderChange,
        onColumnPinningChange: columns.onColumnPinningChange,
        enableRowSelection: true,
        getCoreRowModel: getCoreRowModel(),
    });

    const { openSheet: openColumnSettings } = useTableDisplaySettingsSheet(table, columns.settingsOptions);

    const refresh = () => {
        fullDocumentProvider.clearCache();
        resetDataChanged();
        lazyRows.reload();
    };

    const changePagination = (value: boolean) => {
        selection.clear();
        setIsPaginated(value);
    };

    return (
        <div ref={bodyRef} className="vstack min-height-0">
            <DocumentsListToolbar
                collectionName={collectionName}
                isPaginated={isPaginated}
                isCustomLayout={columns.isCustomLayout}
                onPaginationToggle={() => changePagination(!isPaginated)}
                onOpenColumnSettings={openColumnSettings}
                getVisibleColumnFields={() => columns.getExportFields(table)}
                isDataChanged={isDataChanged}
                onDataChangedRefresh={refresh}
            />
            <LazyVirtualTable
                table={table}
                lazyRows={lazyRows}
                isPaginated={isPaginated}
                onIsPaginatedChange={changePagination}
                itemsName="documents"
                emptyMessage={
                    collectionName === null ? "There are no documents in the database" : "Collection is empty"
                }
                bottomOverlay={
                    <DocumentsSelectionActions
                        collectionName={collectionName}
                        collectionDocumentCount={collectionDocumentCount}
                        selection={selection}
                        collectionDeletionCallbacks={collectionDeletionCallbacks}
                        onSelectionDeleted={() => {
                            selection.clear();
                            refresh();
                        }}
                    />
                }
            />
        </div>
    );
}
