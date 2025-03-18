import { useReactTable, getCoreRowModel } from "@tanstack/react-table";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { useVirtualTableWithLazyLoading } from "components/common/virtualTable/hooks/useVirtualTableWithLazyLoading";
import VirtualTableWithLazyLoading from "components/common/virtualTable/VirtualTableWithLazyLoading";
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

    const { dataPreview, componentProps, reload } = useVirtualTableWithLazyLoading({
        fetchData: (skip: number, take: number) => {
            if (databaseName) {
                return databasesService.getRevisionsPreview({
                    databaseName,
                    start: skip,
                    pageSize: take,
                    type: selectedType,
                    collection: selectedCollectionName,
                });
            }
        },
        dependencies: [databaseName, selectedType, selectedCollectionName],
    });

    const columns = useAllRevisionsColumns(databaseName, false, width, selectedRows, setSelectedRows);

    useImperativeHandle(fetcherRef, () => ({
        reload,
    }));

    const table = useReactTable({
        defaultColumn: {
            enableSorting: false,
            enableColumnFilter: false,
        },
        columns,
        data: dataPreview,
        columnResizeMode: "onChange",
        getCoreRowModel: getCoreRowModel(),
    });

    return <VirtualTableWithLazyLoading {...componentProps} table={table} heightInPx={height} />;
}
