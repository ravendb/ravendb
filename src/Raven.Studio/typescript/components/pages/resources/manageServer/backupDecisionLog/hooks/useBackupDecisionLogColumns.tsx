import { CellContext, ColumnDef } from "@tanstack/react-table";
import { virtualTableUtils } from "components/common/virtualTable/utils/virtualTableUtils";
import { BackupDecisionLogEntry } from "../backupDecisionLogTypes";
import { DatabaseCell, EventCell, ReasonCell, TaskCell, TimeCell } from "../partials/BackupDecisionLogCells";

type EntryCellContext = CellContext<BackupDecisionLogEntry, unknown>;

const renderTimeCell = ({ row }: EntryCellContext) => <TimeCell entry={row.original} />;
const renderEventCell = ({ row }: EntryCellContext) => <EventCell entry={row.original} />;
const renderDatabaseCell = ({ row }: EntryCellContext) => <DatabaseCell entry={row.original} />;
const renderTaskCell = ({ row }: EntryCellContext) => <TaskCell entry={row.original} />;
const renderReasonCell = ({ row }: EntryCellContext) => <ReasonCell entry={row.original} />;

export function useBackupDecisionLogColumns(availableWidth: number): ColumnDef<BackupDecisionLogEntry>[] {
    const getSize = virtualTableUtils.getCellSizeProvider(virtualTableUtils.getTableBodyWidth(availableWidth));

    return [
        {
            id: "time",
            header: "Time",
            size: getSize(20),
            accessorFn: (row) => new Date(row.LastTime),
            sortingFn: "datetime",
            enableSorting: true,
            cell: renderTimeCell,
        },
        {
            id: "event",
            header: "Event",
            size: getSize(14),
            cell: renderEventCell,
        },
        {
            id: "database",
            header: "Database",
            size: getSize(16),
            cell: renderDatabaseCell,
        },
        {
            id: "task",
            header: "Task",
            size: getSize(16),
            cell: renderTaskCell,
        },
        {
            id: "reason",
            header: "Reason",
            size: getSize(34),
            cell: renderReasonCell,
        },
    ];
}
