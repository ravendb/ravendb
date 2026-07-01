import React, { useMemo } from "react";
import { Icon } from "components/common/Icon";
import Button from "react-bootstrap/Button";
import {
    ColumnDef,
    getCoreRowModel,
    getFilteredRowModel,
    getSortedRowModel,
    useReactTable,
} from "@tanstack/react-table";
import { virtualTableUtils } from "components/common/virtualTable/utils/virtualTableUtils";
import { virtualTableConstants } from "components/common/virtualTable/utils/virtualTableConstants";
import VirtualTable from "components/common/virtualTable/VirtualTable";
import SizeGetter from "components/common/SizeGetter";
import { CellWithCopyWrapper } from "components/common/virtualTable/cells/CellWithCopy";
import { EmptySet } from "components/common/EmptySet";
import TableDisplaySettings from "components/common/virtualTable/commonComponents/columnsSelect/TableDisplaySettings";
import useBoolean from "hooks/useBoolean";
import { useAppSelector } from "components/store";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import {
    EtlHealthStatus,
    EtlTaskWithErrors,
    FlatError,
    flattenAllTasksErrors,
    SHOW_WIDTH_SIZE,
    TasksFiltersState,
} from "../utils/tasksErrorsUtils";
import {
    CellAffectedDocumentsWrapper,
    CellDateWithRelativeTimeWrapper,
    CellErrorStepWrapper,
    CellErrorTypeWrapper,
    CellEtlTypeWrapper,
    CellNodeValueWrapper,
    CellShardValueWrapper,
    CellTaskHealthWrapper,
    CellTaskWrapper,
    CellValueButtonWrapper,
    HyperLinkDocumentCellValue,
} from "./TasksErrorsCells";
import { DeleteErrorsModal } from "./DeleteModals";
import { accessManagerSelectors } from "components/common/shell/accessManagerSliceSelectors";
import { AccessPopover } from "components/common/AccessPopover";
import EtlTaskStats = Raven.Server.Documents.ETL.Stats.EtlTaskStats;

function useGroupByNoneTableColumns(availableWidth: number, hasProcessErrors: boolean) {
    const db = useAppSelector(databaseSelectors.activeDatabase);
    const bodyWidth = virtualTableUtils.getTableBodyWidth(availableWidth);
    const getSize = useMemo(() => virtualTableUtils.getCellSizeProvider(bodyWidth - SHOW_WIDTH_SIZE), [bodyWidth]);

    const columns = useMemo<ColumnDef<FlatError>[]>(
        () => [
            {
                header: "Show",
                cell: CellValueButtonWrapper,
                size: SHOW_WIDTH_SIZE,
            },
            {
                header: "Task type",
                accessorKey: "etlType",
                cell: CellEtlTypeWrapper,
                size: getSize(5),
                enableSorting: false,
                enableColumnFilter: false,
            },
            {
                header: "Task",
                accessorKey: "etlName",
                cell: CellTaskWrapper,
                size: getSize(18),
                enableColumnFilter: false,
            },
            {
                header: "Error type",
                accessorKey: "errorType",
                cell: CellErrorTypeWrapper,
                size: getSize(8),
            },
            {
                header: "Error step",
                accessorKey: "Step",
                cell: CellErrorStepWrapper,
                size: getSize(8),
            },
            {
                header: "Date",
                accessorKey: "CreatedAt",
                cell: CellDateWithRelativeTimeWrapper,
                size: getSize(15),
            },
            {
                header: "Node",
                accessorKey: "nodeTag",
                cell: CellNodeValueWrapper,
                size: getSize(3),
                enableColumnFilter: false,
                enableSorting: false,
            },
            ...(db.isSharded
                ? [
                      {
                          header: "Shard",
                          accessorKey: "shardNumber",
                          cell: CellShardValueWrapper,
                          size: getSize(3),
                          enableColumnFilter: false,
                          enableSorting: false,
                      },
                  ]
                : []),
            {
                header: "Document",
                accessorKey: "DocumentId",
                cell: HyperLinkDocumentCellValue,
                size: getSize(8),
            },
            ...(hasProcessErrors
                ? [
                      {
                          header: "Affected Documents",
                          accessorKey: "AffectedDocumentsCount",
                          cell: CellAffectedDocumentsWrapper,
                          size: getSize(8),
                      },
                  ]
                : []),
            {
                header: "Current task health",
                accessorKey: "healthStatus",
                cell: CellTaskHealthWrapper,
                size: getSize(8),
                enableColumnFilter: false,
                enableSorting: false,
            },
            {
                header: "Content",
                accessorKey: "Error",
                cell: CellWithCopyWrapper,
                size: getSize((db.isSharded ? 20 : 25) - (hasProcessErrors ? 8 : 0)),
                enableSorting: false,
            },
        ],
        [db.isSharded, getSize, hasProcessErrors]
    );

    return columns;
}

interface GroupByNoneTableProps {
    tasksWithErrors: EtlTaskWithErrors[];
    etlStats: EtlTaskStats[];
    width: number;
    toggleDeleteAllErrorsModal: () => void;
    filters: TasksFiltersState;
}

function GroupByNoneTable({
    tasksWithErrors,
    toggleDeleteAllErrorsModal,
    etlStats,
    width,
    filters,
}: GroupByNoneTableProps) {
    const hasDatabaseWriteAccess = useAppSelector(accessManagerSelectors.getHasDatabaseWriteAccess)();

    const data = useMemo<FlatError[]>(() => {
        const allErrors = flattenAllTasksErrors(tasksWithErrors, etlStats);
        const { searchText, nodeTags, shardNumbers, healthStatuses, taskTypes } = filters;

        return allErrors.filter((error) => {
            const matchesSearch =
                !searchText ||
                error.etlName.toLowerCase().includes(searchText.toLowerCase()) ||
                error.transformationName?.toLowerCase().includes(searchText.toLowerCase());
            const matchesNode = !nodeTags.length || nodeTags.includes(error.nodeTag);
            const matchesShard = !shardNumbers.length || shardNumbers.includes(String(error.shardNumber));
            const matchesHealth =
                !healthStatuses.length || healthStatuses.includes(error.healthStatus as EtlHealthStatus);
            const matchesTaskType = !taskTypes.length || taskTypes.includes(error.etlType);

            return matchesSearch && matchesNode && matchesShard && matchesHealth && matchesTaskType;
        });
    }, [tasksWithErrors, etlStats, filters]);

    const hasProcessErrors = data.some((e) => e.errorType === "Process");
    const columns = useGroupByNoneTableColumns(width, hasProcessErrors);

    const table = useReactTable({
        data,
        columns,
        columnResizeMode: "onChange",
        initialState: {
            sorting: [{ id: "CreatedAt", desc: true }],
        },
        getCoreRowModel: getCoreRowModel(),
        getSortedRowModel: getSortedRowModel(),
        getFilteredRowModel: getFilteredRowModel(),
    });

    if (data.length === 0) {
        return <EmptySet>No tasks match the current filters.</EmptySet>;
    }

    return (
        <div className="d-flex flex-column h-100">
            <div className="d-flex justify-content-between mb-1 flex-shrink-0">
                <AccessPopover accessRequired="DatabaseReadWrite">
                    <Button variant="danger" disabled={!hasDatabaseWriteAccess} onClick={toggleDeleteAllErrorsModal}>
                        <Icon icon="trash" />
                        <span>Delete all errors</span>
                    </Button>
                </AccessPopover>
                <TableDisplaySettings table={table} />
            </div>
            <SizeGetter
                isHeighRequired
                className="flex-grow-1 min-h-0"
                render={({ height }) => (
                    <VirtualTable
                        table={table}
                        heightInPx={height}
                        rowHeightInPx={virtualTableConstants.doubleLineRowHeightInPx}
                    />
                )}
            />
        </div>
    );
}

interface GroupByNoneViewProps {
    tasksWithErrors: EtlTaskWithErrors[];
    etlStats: EtlTaskStats[];
    filters: TasksFiltersState;
    onRefresh: () => void;
}

export function GroupByNoneView({ tasksWithErrors, etlStats, filters, onRefresh }: GroupByNoneViewProps) {
    const { value: isDeleteAllErrorsModalOpen, toggle: toggleDeleteAllErrorsModal } = useBoolean(false);

    return (
        <>
            <div className="d-flex flex-column gap-2 h-100">
                <SizeGetter
                    isHeighRequired
                    className="flex-grow-1 min-h-0"
                    render={({ width }) => (
                        <GroupByNoneTable
                            toggleDeleteAllErrorsModal={toggleDeleteAllErrorsModal}
                            tasksWithErrors={tasksWithErrors}
                            etlStats={etlStats}
                            width={width}
                            filters={filters}
                        />
                    )}
                />
            </div>
            {isDeleteAllErrorsModalOpen && (
                <DeleteErrorsModal
                    mode="all"
                    tasksWithErrors={tasksWithErrors}
                    toggle={toggleDeleteAllErrorsModal}
                    onRefresh={onRefresh}
                />
            )}
        </>
    );
}
