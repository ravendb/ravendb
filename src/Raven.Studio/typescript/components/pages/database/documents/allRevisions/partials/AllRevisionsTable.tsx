import { useReactTable, getCoreRowModel } from "@tanstack/react-table";
import { RevisionsPreviewResultItem } from "commands/database/documents/getRevisionsPreviewCommand";
import { Switch } from "components/common/Checkbox";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { useLazyRows } from "components/common/virtualTable/hooks/useLazyRows";
import LazyVirtualTable from "components/common/virtualTable/LazyVirtualTable";
import { lazyTableOptions } from "components/common/virtualTable/utils/lazyTableUtils";
import { useServices } from "components/hooks/useServices";
import { AllRevisionsTableProps } from "components/pages/database/documents/allRevisions/common/allRevisionsTypes";
import { useAllRevisionsColumns } from "components/pages/database/documents/allRevisions/hooks/useAllRevisionsColumns";
import { useAppSelector } from "components/store";
import { useImperativeHandle, useState } from "react";

export default function AllRevisionsTable({
    width,
    height,
    selectedType,
    selectedCollectionName,
    fetcherRef,
    selectedRows,
    setSelectedRows,
}: AllRevisionsTableProps) {
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

    const [isPaginated, setIsPaginated] = useState(false);

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
            {!isSharded && (
                <div className="d-flex justify-content-end mb-1">
                    <Switch
                        selected={isPaginated}
                        toggleSelection={() => setIsPaginated(!isPaginated)}
                        color="primary"
                        title="Show the revisions page by page instead of scrolling"
                    >
                        Pagination
                    </Switch>
                </div>
            )}
            <LazyVirtualTable
                table={table}
                lazyRows={lazyRows}
                isPaginated={isPaginated}
                onIsPaginatedChange={setIsPaginated}
                itemsName="revisions"
            />
        </div>
    );
}
