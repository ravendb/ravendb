import { useReactTable, getCoreRowModel } from "@tanstack/react-table";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { useLazyVirtualTable } from "components/common/virtualTable/hooks/useLazyVirtualTable";
import { useVirtualTableArea } from "components/common/virtualTable/hooks/useVirtualTableArea";
import LazyVirtualTable from "components/common/virtualTable/LazyVirtualTable";
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

    const area = useVirtualTableArea();

    const lazyTable = useLazyVirtualTable({
        area,
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
        reload: lazyTable.reload,
    }));

    const table = useReactTable({
        columns,
        data: lazyTable.data,
        getCoreRowModel: getCoreRowModel(),
    });

    return (
        <div className="d-flex flex-column" style={{ height }}>
            <LazyVirtualTable lazyTable={lazyTable} table={table} itemsName="revisions" />
        </div>
    );
}
