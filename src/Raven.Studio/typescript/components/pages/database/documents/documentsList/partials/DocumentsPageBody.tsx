import { getCoreRowModel, useReactTable } from "@tanstack/react-table";
import { collectionsTrackerSelectors, systemCollectionNames } from "components/common/shell/collectionsTrackerSlice";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { useTableDisplaySettingsSheet } from "components/common/virtualTable/commonComponents/columnsSelect/TableDisplaySettings";
import { useLazyVirtualTable } from "components/common/virtualTable/hooks/useLazyVirtualTable";
import { useVirtualTableArea } from "components/common/virtualTable/hooks/useVirtualTableArea";
import LazyVirtualTable from "components/common/virtualTable/LazyVirtualTable";
import { useServices } from "components/hooks/useServices";
import { useCollectionRemovalRedirect } from "components/pages/database/documents/documentsList/hooks/useCollectionRemovalRedirect";
import { useDocumentsColumns } from "components/pages/database/documents/documentsList/hooks/useDocumentsColumns";
import { useDocumentsDataChanged } from "components/pages/database/documents/documentsList/hooks/useDocumentsDataChanged";
import {
    DocumentsSelection,
    useDocumentsSelection,
} from "components/pages/database/documents/documentsList/hooks/useDocumentsSelection";
import { useFullDocumentProvider } from "components/pages/database/documents/documentsList/hooks/useFullDocumentProvider";
import DocumentsListToolbar from "components/pages/database/documents/documentsList/partials/DocumentsListToolbar";
import DocumentsSelectionActions from "components/pages/database/documents/documentsList/partials/DocumentsSelectionActions";
import { useAppSelector } from "components/store";
import document from "models/database/documents/document";
import { useRef } from "react";

interface DocumentsPageBodyProps {
    // null means all documents
    collectionName: string | null;
}

export default function DocumentsPageBody({ collectionName }: DocumentsPageBodyProps) {
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const isSharded = useAppSelector(databaseSelectors.isActiveDatabaseSharded);
    const collectionDocumentCount =
        useAppSelector(
            collectionsTrackerSelectors.collectionByName(collectionName ?? systemCollectionNames.allDocuments)
        )?.documentCount ?? null;
    const { databasesService } = useServices();

    const { isDataChanged, trackResultEtag, reset: resetDataChanged } = useDocumentsDataChanged(collectionName);
    const fullDocumentProvider = useFullDocumentProvider(databaseName);
    const collectionDeletionCallbacks = useCollectionRemovalRedirect({ databaseName, collectionName });

    // the selection is created after the rows it describes, the column defs reach it through the ref
    const selectionRef = useRef<DocumentsSelection>(null);

    const area = useVirtualTableArea();

    const columns = useDocumentsColumns({
        databaseName,
        collectionName,
        tableBodyWidthInPx: area.bodyWidthInPx,
        selectionRef,
        fullDocumentProvider,
    });

    const lazyTable = useLazyVirtualTable<document>({
        area,
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
        reloadDependencies: [databaseName, collectionName, isSharded, columns.previewBindings, columns.fullBindings],
    });

    const selection = useDocumentsSelection({
        rows: lazyTable.rows,
        totalCount: collectionDocumentCount ?? lazyTable.totalCount,
        isPaginated: lazyTable.isPaginated,
    });
    selectionRef.current = selection;

    const table = useReactTable({
        data: lazyTable.data,
        columns: columns.columnDefs,
        getRowId: (doc) => doc.getId(),
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
        lazyTable.reload();
    };

    return (
        <>
            <DocumentsListToolbar
                collectionName={collectionName}
                isPaginated={lazyTable.isPaginated}
                isCustomLayout={columns.isCustomLayout}
                onPaginationToggle={() => {
                    selection.clear();
                    lazyTable.setIsPaginated(!lazyTable.isPaginated);
                }}
                onOpenColumnSettings={openColumnSettings}
                getVisibleColumnFields={() => columns.getExportFields(table)}
                isDataChanged={isDataChanged}
                onDataChangedRefresh={refresh}
            />
            <LazyVirtualTable
                lazyTable={lazyTable}
                table={table}
                itemsName="documents"
                emptyMessage={
                    collectionName === null ? "There are no documents in the database" : "Collection is empty"
                }
                selectionAnchorRowIndex={selection.anchorRowIndex}
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
        </>
    );
}
