import { getCoreRowModel, getSortedRowModel, useReactTable } from "@tanstack/react-table";
import VirtualTable from "components/common/virtualTable/VirtualTable";
import { BackupDecisionLogEntry } from "../backupDecisionLogTypes";
import { useBackupDecisionLogColumns } from "../hooks/useBackupDecisionLogColumns";

interface BackupDecisionLogTableProps {
    entries: BackupDecisionLogEntry[];
}

export default function BackupDecisionLogTable({ entries }: BackupDecisionLogTableProps) {
    const columns = useBackupDecisionLogColumns();

    const table = useReactTable({
        defaultColumn: { enableSorting: false, enableColumnFilter: false },
        initialState: { sorting: [{ id: "time", desc: true }] },
        columns,
        data: entries,
        getRowId: (row) => String(row.Id),
        getCoreRowModel: getCoreRowModel(),
        getSortedRowModel: getSortedRowModel(),
    });

    return <VirtualTable table={table} heightInPx={600} rowHeightInPx={56} />;
}
