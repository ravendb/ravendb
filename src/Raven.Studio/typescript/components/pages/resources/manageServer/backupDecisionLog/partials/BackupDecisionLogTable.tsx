import { getCoreRowModel, getSortedRowModel, useReactTable } from "@tanstack/react-table";
import VirtualTable from "components/common/virtualTable/VirtualTable";
import { BackupDecisionRow } from "../backupDecisionLogTypes";
import { useBackupDecisionLogColumns } from "../hooks/useBackupDecisionLogColumns";

interface BackupDecisionLogTableProps {
    rows: BackupDecisionRow[];
}

export default function BackupDecisionLogTable({ rows }: BackupDecisionLogTableProps) {
    const columns = useBackupDecisionLogColumns();

    const table = useReactTable({
        defaultColumn: { enableSorting: false, enableColumnFilter: false },
        initialState: { sorting: [{ id: "time", desc: true }] },
        columns,
        data: rows,
        getRowId: (row) => row.Id,
        getCoreRowModel: getCoreRowModel(),
        getSortedRowModel: getSortedRowModel(),
    });

    return <VirtualTable table={table} heightInPx={600} />;
}
