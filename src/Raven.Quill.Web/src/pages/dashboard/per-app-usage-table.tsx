/* eslint-disable react-hooks/incompatible-library */
// https://github.com/TanStack/table/issues/5567
"use no memo";

import { useMemo, useState } from "react";
import {
    getCoreRowModel,
    getExpandedRowModel,
    useReactTable,
    type ColumnDef,
    type ExpandedState,
    type Row,
} from "@tanstack/react-table";
import { ChevronRight } from "lucide-react";
import type { QuillApplicationUsage } from "@/api/generated/server-api";
import { InfoHint } from "@/components/data/info-hint";
import { WruLabel } from "@/components/data/wru-label";
import { CountBadge } from "@/components/data/count-badge";
import { VirtualDataTable, VirtualDataTableSkeleton } from "@/components/table/virtual-data-table";
import { cn } from "@/lib/utils";
import { rowKey, SYSTEM_GROUP_DESCRIPTION, toUsageGroups, type UsageGroup } from "@/pages/dashboard/usage-groups";

// Groups are the top-level rows; the databases behind them are the sub-rows.
type UsageRow = UsageGroup | QuillApplicationUsage;

function isGroup(row: UsageRow): row is UsageGroup {
    return "rows" in row;
}

// Reserved on every row, expandable or not, so the names line up in one column.
const CHEVRON_GUTTER = "size-3.5 shrink-0";

// `leading-none` is load-bearing: Inter's font box sits high in a text-sm line box, so an icon
// centred on that line box reads low against the letters. Collapsing the line box centres both.
const NAME_ROW = "flex h-5 items-center gap-1.5 leading-none";

const ROW_HEIGHT_IN_PX = 44;

const NO_USAGE: UsageRow[] = [];

// The usage column opts out of resizing, which is what keeps it at this width: auto-sizing hands
// the leftover container width to the resizable columns, so the name column is the one that grows.
const USAGE_COLUMN_WIDTH_IN_PX = 110;

function headClassName(columnId: string) {
    return cn("text-xs font-medium text-muted-foreground", columnId === "usage" && "justify-end");
}

function cellClassName(columnId: string, depth: number) {
    if (columnId === "usage") {
        return "justify-end text-muted-foreground tabular-nums";
    }

    return depth > 0 ? "font-mono text-xs text-muted-foreground" : "font-medium";
}

// A member indents by the same gutter its group spends on the chevron, so its id lines up under the
// name above it. The indent sits inside the cell rather than in its padding: column auto-sizing
// measures the content and assumes the default cell padding, so padding the cell instead would
// leave the column short by the difference and clip the ids - which are the whole point of the row.
function MemberName({ topologyId }: { topologyId: string }) {
    return (
        <span className={NAME_ROW}>
            <span aria-hidden="true" className={CHEVRON_GUTTER} />
            {topologyId}
        </span>
    );
}

// The whole row toggles the group; this button carries the state and the keyboard, and lets its
// click bubble to the row so one handler serves both.
function GroupName({
    group,
    isExpandable,
    isExpanded,
}: {
    group: UsageGroup;
    isExpandable: boolean;
    isExpanded: boolean;
}) {
    if (!isExpandable) {
        return (
            <span className={NAME_ROW}>
                <span aria-hidden="true" className={CHEVRON_GUTTER} />
                {group.label}
            </span>
        );
    }

    return (
        <span className={NAME_ROW}>
            <button
                type="button"
                aria-expanded={isExpanded}
                className={cn(
                    "-mx-0.5 flex items-center gap-1.5 rounded-sm px-0.5",
                    "focus-visible:ring-[3px] focus-visible:ring-ring/50 focus-visible:outline-none",
                )}
            >
                <ChevronRight
                    aria-hidden="true"
                    className={cn(
                        CHEVRON_GUTTER,
                        "text-muted-foreground transition-transform",
                        isExpanded && "rotate-90",
                    )}
                />
                {group.label}
            </button>
            {group.isSystem && (
                // The hint explains the group; it doesn't toggle it. `flex` keeps the icon off the
                // text baseline an inline wrapper would put it on, which rides a couple of px high.
                <span className="flex" onClick={(event) => event.stopPropagation()}>
                    <InfoHint content={SYSTEM_GROUP_DESCRIPTION} />
                </span>
            )}
            <CountBadge>{group.rows.length}</CountBadge>
        </span>
    );
}

const COLUMNS: ColumnDef<UsageRow>[] = [
    {
        id: "name",
        header: "Name",
        cell: ({ row }) =>
            isGroup(row.original) ? (
                <GroupName group={row.original} isExpandable={row.getCanExpand()} isExpanded={row.getIsExpanded()} />
            ) : (
                <MemberName topologyId={row.original.topologyId} />
            ),
    },
    {
        id: "usage",
        header: () => <WruLabel />,
        accessorFn: (row) => row.usage,
        cell: ({ getValue }) => getValue<number>().toLocaleString(),
        size: USAGE_COLUMN_WIDTH_IN_PX,
        enableResizing: false,
    },
];

// Only expandable groups hand back sub-rows, so `getCanExpand` answers for the chevron, the cursor
// and the click handler alike.
function getSubRows(row: UsageRow) {
    return isGroup(row) && row.isExpandable ? row.rows : undefined;
}

function getRowId(row: UsageRow, _index: number, parent?: Row<UsageRow>) {
    return isGroup(row) ? row.key : `${parent?.id}/${rowKey(row)}`;
}

export function PerAppUsageTable({ apps }: { apps: QuillApplicationUsage[] }) {
    const [expanded, setExpanded] = useState<ExpandedState>({});
    // "use no memo" opts this file out of the React Compiler, so the memo has to be explicit:
    // react-table resets its row models when `data` changes identity.
    const groups = useMemo<UsageRow[]>(() => toUsageGroups(apps), [apps]);

    const table = useReactTable({
        columns: COLUMNS,
        data: groups,
        getCoreRowModel: getCoreRowModel(),
        getExpandedRowModel: getExpandedRowModel(),
        getSubRows,
        getRowId,
        onExpandedChange: setExpanded,
        state: { expanded },
    });

    // The table addresses cells and rows by id alone, so resolve the styling and the toggles that
    // depend on the row itself up front rather than picking the ids apart again downstream.
    const cellClassNames = new Map<string, string>();
    const toggles = new Map<string, () => void>();
    for (const row of table.getRowModel().rows) {
        if (row.getCanExpand()) {
            toggles.set(row.id, row.getToggleExpandedHandler());
        }
        for (const cell of row.getVisibleCells()) {
            cellClassNames.set(cell.id, cellClassName(cell.column.id, row.depth));
        }
    }

    return (
        <VirtualDataTable
            table={table}
            columnCount={COLUMNS.length}
            emptyMessage="No usage tracked yet."
            className="bg-card"
            rowHeightInPx={ROW_HEIGHT_IN_PX}
            getHeadClassName={headClassName}
            getCellClassName={(cellId) => cellClassNames.get(cellId) ?? ""}
            getRowClassName={(rowId) => (toggles.has(rowId) ? "cursor-pointer" : "")}
            onRowClick={(rowId) => toggles.get(rowId)?.()}
        />
    );
}

export function PerAppUsageTableSkeleton() {
    const table = useReactTable({
        columns: COLUMNS,
        data: NO_USAGE,
        getCoreRowModel: getCoreRowModel(),
    });

    return (
        <VirtualDataTableSkeleton
            table={table}
            rows={4}
            className="bg-card"
            rowHeightInPx={ROW_HEIGHT_IN_PX}
            getHeadClassName={headClassName}
            getCellClassName={(columnId) => cellClassName(columnId, 0)}
        />
    );
}
