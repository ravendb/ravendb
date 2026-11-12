import { ColumnDef } from "@tanstack/react-table";
import { virtualTableUtils } from "components/common/virtualTable/utils/virtualTableUtils";
import { BackupDecisionLogEntry } from "../backupDecisionLogTypes";
import { DatabaseCell, EventCell, ReasonCell, TaskCell, TimeCell } from "../partials/BackupDecisionLogCells";

export function useBackupDecisionLogColumns(): ColumnDef<BackupDecisionLogEntry>[] {
    const getSize = virtualTableUtils.getCellSizeProvider();

    return [
        {
            id: "time",
            header: "Time",
            size: getSize(20),
            accessorFn: (row) => new Date(row.LastTime),
            sortingFn: "datetime",
            enableSorting: true,
            cell: ({ row }) => <TimeCell entry={row.original} />,
        },
        {
            id: "event",
            header: "Event",
            size: getSize(14),
            cell: ({ row }) => <EventCell entry={row.original} />,
        },
        {
            id: "database",
            header: "Database",
            size: getSize(16),
            cell: ({ row }) => <DatabaseCell entry={row.original} />,
        },
        {
            id: "task",
            header: "Task",
            size: getSize(16),
            cell: ({ row }) => <TaskCell entry={row.original} />,
        },
        {
            id: "reason",
            header: "Reason",
            size: getSize(34),
            cell: ({ row }) => <ReasonCell entry={row.original} />,
        },
    ];
}
