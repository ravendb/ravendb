import { useReactTable, getCoreRowModel } from "@tanstack/react-table";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { useVirtualTableWithToken } from "components/common/virtualTable/hooks/useVirtualTableWithToken";
import VirtualTable from "components/common/virtualTable/VirtualTable";
import { useServices } from "components/hooks/useServices";
import { AllRevisionsTableProps } from "components/pages/database/documents/allRevisions/common/allRevisionsTypes";
import { useAllRevisionsColumns } from "components/pages/database/documents/allRevisions/hooks/useAllRevisionsColumns";
import { useAppSelector } from "components/store";
import { useImperativeHandle } from "react";

export default function AllRevisionsTableSharded({
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

    const { dataArray, componentProps, reload } = useVirtualTableWithToken({
        fetchData: (skip: number, take: number, continuationToken?: string) =>
            databasesService.getRevisionsPreview({
                databaseName,
                start: skip,
                pageSize: take,
                continuationToken,
                type: selectedType,
                collection: selectedCollectionName,
            }),
        dependencies: [databaseName, selectedType, selectedCollectionName],
    });

    const columns = useAllRevisionsColumns(databaseName, true, width, selectedRows, setSelectedRows);

    useImperativeHandle(fetcherRef, () => ({
        reload,
    }));

    const table = useReactTable({
        defaultColumn: {
            enableSorting: false,
            enableColumnFilter: false,
        },
        columns,
        data: dataArray,
        columnResizeMode: "onChange",
        getCoreRowModel: getCoreRowModel(),
    });

    return <VirtualTable {...componentProps} table={table} heightInPx={height} />;
}
