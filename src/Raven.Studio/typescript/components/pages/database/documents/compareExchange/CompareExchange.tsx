import { AboutViewHeading } from "components/common/AboutView";
import { Icon } from "components/common/Icon";
import Button from "react-bootstrap/Button";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import CompareExchangeInfoHub from "./CompareExchangeInfoHub";
import SizeGetter from "components/common/SizeGetter";
import { useServices } from "components/hooks/useServices";
import { useVirtualTableWithoutTotalCount } from "components/common/virtualTable/hooks/useVirtualTableWithoutTotalCount";
import { useSelector } from "react-redux";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import VirtualTable from "components/common/virtualTable/VirtualTable";
import {
    ColumnDef,
    ColumnFiltersState,
    getCoreRowModel,
    getSortedRowModel,
    useReactTable,
} from "@tanstack/react-table";
import { virtualTableUtils } from "components/common/virtualTable/utils/virtualTableUtils";
import { CellWithCopy, CellWithCopyWrapper } from "components/common/virtualTable/cells/CellWithCopy";
import { useAppUrls } from "components/hooks/useAppUrls";
import { accessManagerSelectors } from "components/common/shell/accessManagerSliceSelectors";
import { Checkbox } from "components/common/Checkbox";
import { useEffect, useRef, useState } from "react";
import { useEventsCollector } from "components/hooks/useEventsCollector";
import router from "plugins/router";
import useConfirm from "components/common/ConfirmDialog";
import { useAsyncCallback } from "react-async-hook";
import { useAppSelector } from "components/store";
import pluralizeHelpers from "common/helpers/text/pluralizeHelpers";
import { AccessPopover } from "components/common/AccessPopover";

type CompareExchangeListItem =
    Raven.Server.Web.System.Processors.CompareExchange.CompareExchangeHandlerProcessorForGetCompareExchangeValues.CompareExchangeListItem;

const requiredAccess: databaseAccessLevel = "DatabaseReadWrite";

export default function CompareExchange() {
    const databaseName = useSelector(databaseSelectors.activeDatabaseName);
    const canHandleOperation = useAppSelector(accessManagerSelectors.getCanHandleOperation)(requiredAccess);

    const { appUrl } = useAppUrls();

    const { reportEvent } = useEventsCollector();
    const { databasesService } = useServices();
    const confirm = useConfirm();

    const [selectedRows, setSelectedRows] = useState<CompareExchangeListItem[]>([]);
    const [isAllSelected, setIsAllSelected] = useState(false);

    const reloadRef = useRef<() => Promise<void>>(null);
    const keyFilterRef = useRef("");

    const handleAddNewItem = () => {
        reportEvent("cmpXchg", "new");
        const url = appUrl.forNewCmpXchg(databaseName);
        router.navigate(url);
    };

    const asyncDeleteSelected = useAsyncCallback(async () => {
        reportEvent("cmpXchg", "delete");

        const itemsToDelete = isAllSelected
            ? (await databasesService.getCompareExchangeItems(databaseName, keyFilterRef.current, 0, 2147483647)).items
            : selectedRows;

        try {
            await Promise.all(
                itemsToDelete.map((x) => databasesService.deleteCompareExchangeItem(databaseName, x.Key, x.Index))
            );
        } finally {
            // even on partial failure, clear the selection and reload so already-deleted rows disappear
            setSelectedRows([]);
            setIsAllSelected(false);
            await reloadRef.current?.();
        }
    });

    const asyncHandleDelete = useAsyncCallback(async () => {
        const confirmed = isAllSelected
            ? await confirm({
                  title: "Delete ALL compare exchange items?",
                  message: (
                      <span>
                          You&apos;re about to delete <strong>ALL</strong> compare exchange items
                          {keyFilterRef.current ? " matching the current filter" : ""}.
                      </span>
                  ),
                  icon: "trash",
                  actionColor: "danger",
                  confirmText: "Delete All",
              })
            : await confirm({
                  title: `Delete ${pluralizeHelpers.pluralize(
                      selectedRows.length,
                      "compare exchange item",
                      "compare exchange items"
                  )}?`,
                  message: (
                      <ul className="overflow-auto" style={{ maxHeight: "300px" }}>
                          {selectedRows.map((x) => (
                              <li key={x.Key}>{x.Key}</li>
                          ))}
                      </ul>
                  ),
                  icon: "trash",
                  actionColor: "danger",
                  confirmText: "Delete",
              });

        if (confirmed) {
            // delete failures are surfaced by the command layer's error toast (deleteCompareExchangeItemCommand);
            // the rejection is captured by this useAsyncCallback, so it never surfaces as unhandled
            await asyncDeleteSelected.execute();
        }
    });

    return (
        <div className="content-padding vstack h-100">
            <div className="hstack justify-content-between">
                <AboutViewHeading title="Compare Exchange" icon="cmp-xchg" />
                <CompareExchangeInfoHub />
            </div>
            <div className="hstack gap-2">
                <AccessPopover accessRequired={requiredAccess}>
                    <Button variant="primary" onClick={handleAddNewItem} disabled={!canHandleOperation}>
                        <Icon icon="plus" />
                        Add new item
                    </Button>
                </AccessPopover>
                <AccessPopover accessRequired={requiredAccess}>
                    <ButtonWithSpinner
                        variant="danger"
                        icon="trash"
                        onClick={asyncHandleDelete.execute}
                        isSpinning={asyncDeleteSelected.loading}
                        disabled={!canHandleOperation || selectedRows.length === 0 || asyncHandleDelete.loading}
                    >
                        Delete{selectedRows.length > 0 && ` (${isAllSelected ? "all" : selectedRows.length})`}
                    </ButtonWithSpinner>
                </AccessPopover>
            </div>
            <div className="flex-grow overflow-hidden mt-4">
                <SizeGetter
                    render={(props) => (
                        <CompareExchangeTable
                            {...props}
                            selectedRows={selectedRows}
                            setSelectedRows={setSelectedRows}
                            isAllSelected={isAllSelected}
                            setIsAllSelected={setIsAllSelected}
                            reloadRef={reloadRef}
                            keyFilterRef={keyFilterRef}
                        />
                    )}
                />
            </div>
        </div>
    );
}

interface CompareExchangeTableProps {
    width: number;
    height: number;
    selectedRows: CompareExchangeListItem[];
    setSelectedRows: (rows: CompareExchangeListItem[]) => void;
    isAllSelected: boolean;
    setIsAllSelected: (value: boolean) => void;
    reloadRef: React.MutableRefObject<() => Promise<void>>;
    keyFilterRef: React.MutableRefObject<string>;
}

function CompareExchangeTable(props: CompareExchangeTableProps) {
    const { databasesService } = useServices();
    const databaseName = useSelector(databaseSelectors.activeDatabaseName);

    const [columnFilters, setColumnFilters] = useState<ColumnFiltersState>([]);
    const keyFilter = String(columnFilters.find((x) => x.id === "Key")?.value ?? "");

    const { dataArray, reload, componentProps } = useVirtualTableWithoutTotalCount({
        fetchData: (skip, take) => databasesService.getCompareExchangeItems(databaseName, keyFilter, skip, take),
        reloadDependencies: [keyFilter],
    });

    props.reloadRef.current = reload;
    props.keyFilterRef.current = keyFilter;

    // clearing an already-empty selection on mount is a no-op, so no first-render guard is needed
    useEffect(() => {
        props.setSelectedRows([]);
        props.setIsAllSelected(false);
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [keyFilter]);

    useEffect(() => {
        if (props.isAllSelected) {
            props.setSelectedRows(dataArray);
        }
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [dataArray]);

    const columns = useCompareExchangeColumns({
        width: props.width,
        dataArray,
        selectedRows: props.selectedRows,
        setSelectedRows: props.setSelectedRows,
        isAllSelected: props.isAllSelected,
        setIsAllSelected: props.setIsAllSelected,
    });

    const table = useReactTable({
        data: dataArray,
        columns,
        state: { columnFilters },
        onColumnFiltersChange: setColumnFilters,
        getCoreRowModel: getCoreRowModel(),
        getSortedRowModel: getSortedRowModel(),
    });

    return <VirtualTable {...componentProps} heightInPx={props.height} table={table} />;
}

interface UseCompareExchangeColumnsProps {
    width: number;
    dataArray: CompareExchangeListItem[];
    selectedRows: CompareExchangeListItem[];
    setSelectedRows: (rows: CompareExchangeListItem[]) => void;
    isAllSelected: boolean;
    setIsAllSelected: (value: boolean) => void;
}

function useCompareExchangeColumns({
    width,
    dataArray,
    selectedRows,
    setSelectedRows,
    isAllSelected,
    setIsAllSelected,
}: UseCompareExchangeColumnsProps): ColumnDef<CompareExchangeListItem>[] {
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
            header: () => (
                <Checkbox
                    selected={isAllSelected}
                    indeterminate={!isAllSelected && selectedRows.length > 0}
                    toggleSelection={() => {
                        if (isAllSelected || selectedRows.length > 0) {
                            setIsAllSelected(false);
                            setSelectedRows([]);
                        } else {
                            setIsAllSelected(true);
                            setSelectedRows(dataArray);
                        }
                    }}
                />
            ),
            accessorFn: (x) => x,
            cell: ({ getValue }) => (
                <CheckboxCell
                    selectedRows={selectedRows}
                    setSelectedRows={setSelectedRows}
                    setIsAllSelected={setIsAllSelected}
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
            enableSorting: true,
            enableColumnFilter: true,
        },
        {
            id: "Value",
            header: "Value",
            accessorFn: (row) => row.Value["Object"],
            cell: CellWithCopyWrapper,
            size: getSize(20),
            enableSorting: true,
            enableColumnFilter: false,
            sortingFn: "alphanumeric",
        },
        {
            id: "Metadata",
            header: "Metadata",
            accessorFn: (row) => row.Value["@metadata"],
            cell: CellWithCopyWrapper,
            size: getSize(20),
            enableSorting: true,
            enableColumnFilter: false,
            sortingFn: "alphanumeric",
        },
        {
            accessorKey: "Index",
            header: "Raft Index",
            cell: CellWithCopyWrapper,
            size: getSize(20),
            enableSorting: true,
            enableColumnFilter: false,
        }
    );

    return columns;
}

interface CheckboxCellProps {
    rowValue: CompareExchangeListItem;
    selectedRows: CompareExchangeListItem[];
    setSelectedRows: (rows: CompareExchangeListItem[]) => void;
    setIsAllSelected: (value: boolean) => void;
}

function CheckboxCell({ rowValue, selectedRows, setSelectedRows, setIsAllSelected }: CheckboxCellProps) {
    const isSelected = !!selectedRows.find((x) => isRowEqual(x, rowValue));

    const toggleSelection = () => {
        if (isSelected) {
            setIsAllSelected(false);
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
