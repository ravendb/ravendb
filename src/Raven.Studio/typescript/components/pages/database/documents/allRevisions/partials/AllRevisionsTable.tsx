import { useReactTable, getCoreRowModel } from "@tanstack/react-table";
import { RevisionsPreviewResultItem } from "commands/database/documents/getRevisionsPreviewCommand";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { useLazyRows } from "components/common/virtualTable/hooks/useLazyRows";
import LazyVirtualTable from "components/common/virtualTable/LazyVirtualTable";
import { lazyTableOptions } from "components/common/virtualTable/utils/lazyTableUtils";
import { useServices } from "components/hooks/useServices";
import {
    AllRevisionsPaginationProps,
    AllRevisionsTableProps,
} from "components/pages/database/documents/allRevisions/common/allRevisionsTypes";
import { useAllRevisionsColumns } from "components/pages/database/documents/allRevisions/hooks/useAllRevisionsColumns";
import { useAppSelector } from "components/store";
import { useImperativeHandle } from "react";

export default function AllRevisionsTable({
    width,
    height,
    selectedType,
    selectedCollectionName,
    fetcherRef,
    selectedRows,
    setSelectedRows,
    isPaginated,
    onIsPaginatedChange,
}: AllRevisionsTableProps & AllRevisionsPaginationProps) {
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const isSharded = useAppSelector(databaseSelectors.activeDatabase)?.isSharded;
    const { databasesService } = useServices();

    const lazyRows = useLazyRows<RevisionsPreviewResultItem>({
        fetchMode: isSharded ? "continuationToken" : "skipTake",
        fetchData: (skip: number, take: number, continuationToken?: string) =>
            databasesService.getRevisionsPreview({
                databaseName,
                start: skip,
                pageSize: take,
                continuationToken,
                type: selectedType,
                collection: selectedCollectionName,
            }),
        reloadDependencies: [databaseName, selectedType, selectedCollectionName],
    });

    const columns = useAllRevisionsColumns(databaseName, isSharded, width, selectedRows, setSelectedRows);

    useImperativeHandle(fetcherRef, () => ({
        reload: lazyRows.reload,
    }));

    const table = useReactTable({
        ...lazyTableOptions,
        columns,
        data: lazyRows.data,
        getCoreRowModel: getCoreRowModel(),
    });

    return (
        <div className="d-flex flex-column" style={{ height }}>
            <LazyVirtualTable
                table={table}
                lazyRows={lazyRows}
                isPaginated={isPaginated}
                onIsPaginatedChange={onIsPaginatedChange}
                itemsName="revisions"
            />
        </div>
    );
}
