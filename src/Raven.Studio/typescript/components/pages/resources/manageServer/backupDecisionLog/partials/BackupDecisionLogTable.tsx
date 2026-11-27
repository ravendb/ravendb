import "./BackupDecisionLogTable.scss";
import { getCoreRowModel, getSortedRowModel, useReactTable } from "@tanstack/react-table";
import SizeGetter from "components/common/SizeGetter";
import VirtualTable from "components/common/virtualTable/VirtualTable";
import { BackupDecisionLogEntry } from "../backupDecisionLogTypes";
import { useBackupDecisionLogColumns } from "../hooks/useBackupDecisionLogColumns";

interface BackupDecisionLogTableProps {
    entries: BackupDecisionLogEntry[];
}

export default function BackupDecisionLogTable({ entries }: BackupDecisionLogTableProps) {
    return (
        <SizeGetter
            isHeighRequired
            className="backup-decision-log-table"
            render={({ width, height }) => (
                <BackupDecisionLogTableWithSize entries={entries} width={width} height={height} />
            )}
        />
    );
}

interface BackupDecisionLogTableWithSizeProps extends BackupDecisionLogTableProps {
    width: number;
    height: number;
}

function BackupDecisionLogTableWithSize({ entries, width, height }: BackupDecisionLogTableWithSizeProps) {
    const columns = useBackupDecisionLogColumns(width);

    const table = useReactTable({
        defaultColumn: { enableSorting: false, enableColumnFilter: false },
        initialState: { sorting: [{ id: "time", desc: true }] },
        columns,
        data: entries,
        getRowId: (row) => String(row.Id),
        getCoreRowModel: getCoreRowModel(),
        getSortedRowModel: getSortedRowModel(),
    });

    return <VirtualTable table={table} heightInPx={height} rowHeightInPx={56} isRowHeightDynamic />;
}
