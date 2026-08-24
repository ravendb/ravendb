/* eslint-disable react-hooks/incompatible-library */
// https://github.com/TanStack/table/issues/5567
"use no memo";

import { useMemo, useState } from "react";
import {
    flexRender,
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
import { Badge } from "@/components/shadcn/ui/badge";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/shadcn/ui/table";
import { cn } from "@/lib/utils";
import { rowKey, SYSTEM_GROUP_DESCRIPTION, toUsageGroups, type UsageGroup } from "@/pages/dashboard/usage-groups";

// A group and the databases behind it share one table: the groups are the top-level rows, and their
// databases the sub-rows react-table expands into.
type UsageRow = UsageGroup | QuillApplicationUsage;

function isGroup(row: UsageRow): row is UsageGroup {
    return "rows" in row;
}

// The name column reserves the chevron's width on every row, expandable or not, so all the names
// line up in one column and the chevrons sit in a gutter of their own.
const CHEVRON_GUTTER = "size-3.5 shrink-0";

// Inter's font box sits high in a text-sm line box - more of the leading falls below the letters
// than above - so an icon centred on that line box reads a hair low against them. `leading-none`
// collapses the line box onto the font box, which centres the letters and the chevron on the same
// axis, and the fixed height keeps every row as tall as it was.
const NAME_ROW = "flex h-5 items-center gap-1.5 leading-none";

// Where the names land: pl-4 + the gutter + the gap. Members of an expanded group indent to it, so
// they read as sitting under the name they belong to.
const NAME_INDENT = "pl-9";

// Layout that belongs to the cell rather than to what it holds, so it stays on the <th>/<td> the
// column renders into instead of on a wrapper inside it.
function headClassName(columnId: string) {
    return cn("text-xs font-medium text-muted-foreground", columnId === "name" ? "w-full pl-4" : "pr-4 text-right");
}

function cellClassName(columnId: string, depth: number) {
    const isMember = depth > 0;

    if (columnId === "usage") {
        return cn("pr-4 text-right text-muted-foreground tabular-nums", isMember ? "py-2" : "py-3");
    }

    return isMember ? cn("py-2 font-mono text-xs text-muted-foreground", NAME_INDENT) : "py-3 pl-4 font-medium";
}

// The group's own name cell: its shared name (or "System") and how many rows it stands for. The
// whole row toggles it; the button carries the state and the keyboard, and lets its click bubble to
// the row so one handler serves both. Groups standing for a single app have nothing to expand and
// render as the plain name they always were.
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
                // The hint explains the group; it doesn't toggle it.
                <span onClick={(event) => event.stopPropagation()}>
                    <InfoHint content={SYSTEM_GROUP_DESCRIPTION} />
                </span>
            )}
            <Badge variant="secondary" className="tabular-nums">
                {group.rows.length}
            </Badge>
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
                // One row per database behind an expanded group, labelled by the topology id - the
                // only thing that tells apart rows that share a name.
                row.original.topologyId
            ),
    },
    {
        id: "usage",
        header: () => <WruLabel />,
        accessorFn: (row) => row.usage,
        cell: ({ getValue }) => getValue<number>().toLocaleString(),
    },
];

// Only expandable groups hand react-table sub-rows, so `getCanExpand` answers for the chevron, the
// cursor and the click handler alike, without a second notion of expandability alongside it.
function getSubRows(row: UsageRow) {
    return isGroup(row) && row.isExpandable ? row.rows : undefined;
}

function getRowId(row: UsageRow, _index: number, parent?: Row<UsageRow>) {
    return isGroup(row) ? row.key : `${parent?.id}/${rowKey(row)}`;
}

export function PerAppUsageTable({ apps }: { apps: QuillApplicationUsage[] }) {
    const [expanded, setExpanded] = useState<ExpandedState>({});
    // react-table needs a stable data reference between renders; a fresh identity on every render
    // makes it recompute row models and queue state resets. "use no memo" opts this file out of the
    // React Compiler (incompatible with react-table), so the explicit useMemo matters.
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

    const rows = table.getRowModel().rows;

    return (
        <Table>
            <TableHeader>
                {table.getHeaderGroups().map((headerGroup) => (
                    <TableRow key={headerGroup.id} className="hover:bg-transparent">
                        {headerGroup.headers.map((header) => (
                            <TableHead key={header.id} className={headClassName(header.column.id)}>
                                {flexRender(header.column.columnDef.header, header.getContext())}
                            </TableHead>
                        ))}
                    </TableRow>
                ))}
            </TableHeader>
            <TableBody>
                {rows.length === 0 ? (
                    <TableRow className="hover:bg-transparent">
                        <TableCell colSpan={COLUMNS.length} className="h-20 text-center text-muted-foreground">
                            No usage tracked yet.
                        </TableCell>
                    </TableRow>
                ) : (
                    rows.map((row) => (
                        <TableRow
                            key={row.id}
                            onClick={row.getCanExpand() ? row.getToggleExpandedHandler() : undefined}
                            className={cn(
                                row.getCanExpand() && "cursor-pointer",
                                row.depth > 0 && "hover:bg-transparent",
                            )}
                        >
                            {row.getVisibleCells().map((cell) => (
                                <TableCell key={cell.id} className={cellClassName(cell.column.id, row.depth)}>
                                    {flexRender(cell.column.columnDef.cell, cell.getContext())}
                                </TableCell>
                            ))}
                        </TableRow>
                    ))
                )}
            </TableBody>
        </Table>
    );
}
