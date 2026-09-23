import { useReactTable, getCoreRowModel } from "@tanstack/react-table";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { useLazyRows } from "components/common/virtualTable/hooks/useLazyRows";
import LazyVirtualTable from "components/common/virtualTable/LazyVirtualTable";
import { lazyTableOptions } from "components/common/virtualTable/utils/lazyTableUtils";
import { useServices } from "components/hooks/useServices";
import { AllRevisionsTableProps } from "components/pages/database/documents/allRevisions/common/allRevisionsTypes";
import { useAllRevisionsColumns } from "components/pages/database/documents/allRevisions/hooks/useAllRevisionsColumns";
import { useAppSelector } from "components/store";
import { useImperativeHandle } from "react";

export default function AllRevisionsTableNonSharded({
    width,
    height,
    selectedType,
    selectedCollectionName,
    fetcherRef,
    selectedRows,
    setSelectedRows,
}: AllRevisionsTableProps) {
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const { databasesService } = useServices();

    const lazyRows = useLazyRows({
        fetchData: (skip: number, take: number) =>
            databasesService.getRevisionsPreview({
                databaseName,
                start: skip,
                pageSize: take,
                type: selectedType,
                collection: selectedCollectionName,
            }),
        reloadDependencies: [databaseName, selectedType, selectedCollectionName],
    });

    const columns = useAllRevisionsColumns(databaseName, false, width, selectedRows, setSelectedRows);

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
            <LazyVirtualTable table={table} lazyRows={lazyRows} itemsName="revisions" />
        </div>
    );
}
