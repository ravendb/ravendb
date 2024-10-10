import { useReactTable, getCoreRowModel, ColumnDef } from "@tanstack/react-table";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import SizeGetter from "components/common/SizeGetter";
import { virtualTableUtils } from "components/common/virtualTable/utils/virtualTableUtils";
import { useServices } from "components/hooks/useServices";
import { useAppSelector } from "components/store";
import CellDocumentValue from "components/common/virtualTable/cells/CellDocumentValue";
import { CellWithCopyWrapper } from "components/common/virtualTable/cells/CellWithCopy";
import { Icon } from "components/common/Icon";
import VirtualTable from "components/common/virtualTable/VirtualTable";
import { useVirtualTableWithToken } from "components/common/virtualTable/hooks/useVirtualTableWithToken";
import VirtualTableWithLazyLoading from "components/common/virtualTable/VirtualTableWithLazyLoading";
import { RevisionsPreviewResultItem } from "commands/database/documents/getRevisionsPreviewCommand";
import { useMemo } from "react";
import { useVirtualTableWithLazyLoading } from "components/common/virtualTable/hooks/useVirtualTableWithLazyLoading";

interface AllRevisionsWithSizeProps {
    width: number;
    height: number;
}

export default function AllRevisions() {
    return (
        <div className="content-padding">
            <SizeGetter
                isHeighRequired
                render={({ width, height }) => <AllRevisionsWithSize width={width} height={height} />}
            />
        </div>
    );
}

function AllRevisionsWithSize({ width, height }: AllRevisionsWithSizeProps) {
    const isSharded = useAppSelector(databaseSelectors.activeDatabase)?.isSharded;

    const tableProps = {
        width: virtualTableUtils.getTableBodyWidth(width),
        height: height,
    };

    return isSharded ? <AllRevisionsTableSharded {...tableProps} /> : <AllRevisionsTableNonSharded {...tableProps} />;
}

function AllRevisionsTableNonSharded({ width, height }: AllRevisionsWithSizeProps) {
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const { databasesService } = useServices();

    const { dataPreview, componentProps } = useVirtualTableWithLazyLoading({
        fetchData: (skip: number, take: number) => {
            if (databaseName) {
                return databasesService.getRevisionsPreview(databaseName, skip, take);
            }
        },
    });

    const columns = useMemo(() => getColumnDefs(databaseName, width, false), [databaseName, width]);

    const table = useReactTable({
        defaultColumn: {
            enableSorting: false,
        },
        columns,
        data: dataPreview,
        columnResizeMode: "onChange",
        getCoreRowModel: getCoreRowModel(),
    });

    return <VirtualTableWithLazyLoading {...componentProps} table={table} heightInPx={height} />;
}

function AllRevisionsTableSharded({ width, height }: AllRevisionsWithSizeProps) {
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const { databasesService } = useServices();

    const { dataArray, componentProps } = useVirtualTableWithToken({
        fetchData: (skip: number, take: number, continuationToken?: string) =>
            databasesService.getRevisionsPreview(databaseName, skip, take, continuationToken),
    });

    const columns = useMemo(() => getColumnDefs(databaseName, width, true), [databaseName, width]);

    const table = useReactTable({
        defaultColumn: {
            enableSorting: false,
        },
        columns,
        data: dataArray,
        columnResizeMode: "onChange",
        getCoreRowModel: getCoreRowModel(),
    });

    return <VirtualTable {...componentProps} table={table} heightInPx={height} />;
}

const getColumnDefs = (
    databaseName: string,
    tableBodyWidth: number,
    isSharded?: boolean
): ColumnDef<RevisionsPreviewResultItem>[] => {
    const sizeProvider = virtualTableUtils.getCellSizeProvider(tableBodyWidth);

    const columns: ColumnDef<RevisionsPreviewResultItem>[] = [
        {
            accessorKey: "Id",
            cell: ({ getValue }) => (
                <CellDocumentValue value={getValue<string>()} databaseName={databaseName} hasHyperlinkForIds />
            ),
            size: sizeProvider(30),
        },
        {
            accessorKey: "Etag",
            cell: CellWithCopyWrapper,
            size: sizeProvider(10),
        },
        {
            header: "Change Vector",
            accessorKey: "ChangeVector",
            cell: CellWithCopyWrapper,
            size: sizeProvider(25),
        },
        {
            header: "Last Modified",
            accessorKey: "LastModified",
            cell: CellWithCopyWrapper,
            size: sizeProvider(25),
        },
    ];

    if (isSharded) {
        columns.push({
            id: "ShardNumber",
            header: () => (
                <span>
                    <Icon icon="shard" /> Shard
                </span>
            ),
            accessorKey: "ShardNumber",
            cell: CellWithCopyWrapper,
            size: sizeProvider(10),
        });
    }

    return columns;
};
