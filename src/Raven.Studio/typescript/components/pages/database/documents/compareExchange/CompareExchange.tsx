import { AboutViewHeading } from "components/common/AboutView";
import { HStack } from "components/common/HStack";
import { Icon } from "components/common/Icon";
import Button from "react-bootstrap/Button";
import CompareExchangeInfoHub from "./CompareExchangeInfoHub";
import SizeGetter from "components/common/SizeGetter";
import { useServices } from "components/hooks/useServices";
import { useVirtualTableWithoutTotalCount } from "components/common/virtualTable/hooks/useVirtualTableWithoutTotalCount";
import { useSelector } from "react-redux";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import VirtualTable from "components/common/virtualTable/VirtualTable";
import { ColumnDef, getCoreRowModel, useReactTable } from "@tanstack/react-table";
import { virtualTableUtils } from "components/common/virtualTable/utils/virtualTableUtils";
import { CellWithCopy, CellWithCopyWrapper } from "components/common/virtualTable/cells/CellWithCopy";
import { useAppUrls } from "components/hooks/useAppUrls";
import { accessManagerSelectors } from "components/common/shell/accessManagerSliceSelectors";
import { Checkbox } from "components/common/Checkbox";
import { useState } from "react";
import { useEventsCollector } from "components/hooks/useEventsCollector";
import router from "plugins/router";

type CompareExchangeListItem =
    Raven.Server.Web.System.Processors.CompareExchange.CompareExchangeHandlerProcessorForGetCompareExchangeValues.CompareExchangeListItem;

export default function CompareExchange() {
    const databaseName = useSelector(databaseSelectors.activeDatabaseName);
    const hasDatabaseWriteAccess = useSelector(accessManagerSelectors.getHasDatabaseWriteAccess)();

    // TODO maybe pass key filter as custom component to column filter for virtual table

    const { appUrl } = useAppUrls();

    const { reportEvent } = useEventsCollector();

    const [selectedRows, setSelectedRows] = useState<CompareExchangeListItem[]>([]);

    const handleAddNewItem = (e: React.MouseEvent<HTMLButtonElement>) => {
        reportEvent("cmpXchg", "new");
        const url = appUrl.forNewCmpXchg(databaseName);
        if (e.ctrlKey) {
            window.open(url);
        } else {
            router.navigate(url);
        }
    };
    // TODO test selecting all or all w/o one

    return (
        <div className="content-padding vstack">
            <HStack className="justify-content-between">
                <AboutViewHeading title="Compare Exchange" icon="cmp-xchg" />
                <CompareExchangeInfoHub />
            </HStack>
            {hasDatabaseWriteAccess && (
                <HStack className="gap-2">
                    <Button variant="primary" onClick={handleAddNewItem}>
                        <Icon icon="plus" />
                        Add new item
                    </Button>
                    <Button variant="danger">
                        <Icon icon="trash" />
                        Delete
                    </Button>
                </HStack>
            )}
            <div className="flex-grow mt-4">
                <SizeGetter
                    render={(props) => (
                        <CompareExchangeTable
                            {...props}
                            selectedRows={selectedRows}
                            setSelectedRows={setSelectedRows}
                        />
                    )}
                />
            </div>
        </div>
    );
}

function CompareExchangeTable(props: {
    width: number;
    height: number;
    selectedRows: CompareExchangeListItem[];
    setSelectedRows: (rows: CompareExchangeListItem[]) => void;
}) {
    const { databasesService } = useServices();
    const databaseName = useSelector(databaseSelectors.activeDatabaseName);

    const { dataArray, componentProps } = useVirtualTableWithoutTotalCount({
        fetchData: async (skip, take) => {
            // todo filter
            const result = await databasesService.getCompareExchangeItems(databaseName, "", skip, take);
            return result;
        },
    });

    const columns = useCompareExchangeColumns(props.width, props.selectedRows, props.setSelectedRows);

    const table = useReactTable({
        data: dataArray,
        columns,
        getCoreRowModel: getCoreRowModel(),
    });

    return <VirtualTable {...componentProps} heightInPx={props.height} table={table} />;
}

function useCompareExchangeColumns(
    width: number,
    selectedRows: CompareExchangeListItem[],
    setSelectedRows: (rows: CompareExchangeListItem[]) => void
): ColumnDef<CompareExchangeListItem>[] {
    const databaseName = useSelector(databaseSelectors.activeDatabaseName);
    const hasDatabaseWriteAccess = useSelector(accessManagerSelectors.getHasDatabaseWriteAccess)();

    const checkboxWidth = hasDatabaseWriteAccess ? 38 : 0;

    const bodyWidth = virtualTableUtils.getTableBodyWidth(width - checkboxWidth);
    const getSize = virtualTableUtils.getCellSizeProvider(bodyWidth);

    const { appUrl } = useAppUrls();

    const columns: ColumnDef<CompareExchangeListItem>[] = [];

    if (hasDatabaseWriteAccess) {
        columns.push({
            id: "Checkbox",
            header: "",
            accessorFn: (x) => x,
            cell: ({ getValue }) => (
                <CheckboxCell
                    selectedRows={selectedRows}
                    setSelectedRows={setSelectedRows}
                    rowValue={getValue<CompareExchangeListItem>()}
                />
            ),
            size: checkboxWidth,
            minSize: checkboxWidth,
            enableSorting: false,
            enableHiding: false,
            enableColumnFilter: false,
        });
    }

    columns.push(
        {
            accessorKey: "Key",
            header: "Compare Exchange Key",
            cell: ({ getValue }) => {
                const value = getValue<string>();

                return (
                    <CellWithCopy value={value}>
                        <a href={appUrl.forEditCmpXchg(value, databaseName)}>{String(value)}</a>
                    </CellWithCopy>
                );
            },
            size: getSize(40),
        },
        {
            header: "Value",
            accessorFn: (row) => row.Value["Object"],
            cell: CellWithCopyWrapper,
            size: getSize(20),
        },
        {
            header: "Metadata",
            accessorFn: (row) => row.Value["@metadata"],
            cell: CellWithCopyWrapper,
            size: getSize(20),
        },
        {
            header: "Raft Index",
            accessorKey: "Index",
            cell: CellWithCopyWrapper,
            size: getSize(20),
        }
    );

    return columns;
}

interface CheckboxCellProps {
    rowValue: CompareExchangeListItem;
    selectedRows: CompareExchangeListItem[];
    setSelectedRows: (rows: CompareExchangeListItem[]) => void;
}

function CheckboxCell({ rowValue, selectedRows, setSelectedRows }: CheckboxCellProps) {
    const isSelected = !!selectedRows.find((x) => isRowEqual(x, rowValue));

    const toggleSelection = () => {
        if (isSelected) {
            setSelectedRows(selectedRows.filter((x) => !isRowEqual(x, rowValue)));
        } else {
            setSelectedRows([...selectedRows, rowValue]);
        }
    };

    return <Checkbox selected={isSelected} toggleSelection={toggleSelection} />;
}

function isRowEqual(a: CompareExchangeListItem, b: CompareExchangeListItem): boolean {
    return a.Key === b.Key;
}
