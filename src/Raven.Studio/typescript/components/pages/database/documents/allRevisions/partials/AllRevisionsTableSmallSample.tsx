import { useReactTable, getCoreRowModel } from "@tanstack/react-table";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import VirtualTable from "components/common/virtualTable/VirtualTable";
import { useServices } from "components/hooks/useServices";
import { AllRevisionsTableProps } from "components/pages/database/documents/allRevisions/common/allRevisionsTypes";
import { allRevisionsUtils } from "components/pages/database/documents/allRevisions/common/allRevisionsUtils";
import { useAllRevisionsColumns } from "components/pages/database/documents/allRevisions/hooks/useAllRevisionsColumns";
import { useAppSelector } from "components/store";
import { useImperativeHandle, useMemo } from "react";
import { useAsync } from "react-async-hook";

export default function AllRevisionsTableSmallSample({
    width,
    height,
    selectedType,
    selectedCollectionName,
    fetcherRef,
    selectedRows,
    setSelectedRows,
}: AllRevisionsTableProps) {
    const db = useAppSelector(databaseSelectors.activeDatabase);
    const { databasesService } = useServices();

    const asyncGetRevisionsPreview = useAsync(
        () =>
            databasesService.getRevisionsPreview({
                databaseName: db.name,
                start: 0,
                pageSize: allRevisionsUtils.smallSampleSize,
                type: selectedType,
                collection: selectedCollectionName,
            }),
        [db.name, selectedType, selectedCollectionName, allRevisionsUtils.smallSampleSize]
    );

    const columns = useAllRevisionsColumns(db.name, db.isSharded, width, selectedRows, setSelectedRows);

    const data = useMemo(() => asyncGetRevisionsPreview.result?.items ?? [], [asyncGetRevisionsPreview.result]);

    useImperativeHandle(fetcherRef, () => ({
        reload: asyncGetRevisionsPreview.execute,
    }));

    const table = useReactTable({
        defaultColumn: {
            enableSorting: false,
            enableColumnFilter: false,
        },
        columns,
        data,
        columnResizeMode: "onChange",
        getCoreRowModel: getCoreRowModel(),
    });

    return <VirtualTable table={table} heightInPx={height} />;
}
