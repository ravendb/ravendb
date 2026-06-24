/* eslint-disable react-hooks/incompatible-library */
// https://github.com/TanStack/table/issues/5567
"use no memo";

import { useRef, type CSSProperties, type ReactNode } from "react";
import { flexRender, type Header, type Table as ReactTable } from "@tanstack/react-table";
import { useVirtualizer } from "@tanstack/react-virtual";
import { TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/shadcn/ui/table";
import { useAutoSizeColumns } from "@/components/table/use-auto-size-columns";
import { cn } from "@/lib/utils";

const DEFAULT_TABLE_HEIGHT_IN_PX = 360;
const DEFAULT_ROW_HEIGHT_IN_PX = 42;
const DEFAULT_OVERSCAN = 8;

interface VirtualDataTableProps<TData> {
    table: ReactTable<TData>;
    columnCount: number;
    emptyMessage: string;
    className?: string;
    heightInPx?: number;
    overscan?: number;
    rowHeightInPx?: number;
    getCellClassName?: (cellId: string) => string;
    getRowState?: (rowId: string) => string;
    /** Floating content laid over the table region, e.g. a selection toolbar pinned to the bottom edge. */
    overlay?: ReactNode;
}

function getColumnStyle(size: number): CSSProperties {
    return { width: size, flexGrow: 0, flexShrink: 0 };
}

export function VirtualDataTable<TData>({
    table,
    columnCount,
    emptyMessage,
    className,
    heightInPx = DEFAULT_TABLE_HEIGHT_IN_PX,
    overscan = DEFAULT_OVERSCAN,
    rowHeightInPx = DEFAULT_ROW_HEIGHT_IN_PX,
    getCellClassName,
    getRowState,
    overlay,
}: VirtualDataTableProps<TData>) {
    // Enable resizing for the whole table so the drag handles and content-based auto-sizing
    // work without every caller having to opt in.
    table.setOptions((prev) => ({
        ...prev,
        enableColumnResizing: true,
        columnResizeMode: "onChange",
    }));

    const tableContainerRef = useRef<HTMLDivElement>(null);
    const rows = table.getRowModel().rows;

    const rowVirtualizer = useVirtualizer({
        count: rows.length,
        estimateSize: () => rowHeightInPx,
        getScrollElement: () => tableContainerRef.current,
        overscan,
    });

    useAutoSizeColumns(table, tableContainerRef, rows.length);

    if (rows.length === 0) {
        return (
            <div className={cn("min-w-0 overflow-hidden rounded-lg border", className)}>
                <table className="w-full caption-bottom text-sm">
                    <VirtualTableHeader table={table} />
                    <TableBody>
                        <TableRow>
                            <TableCell colSpan={columnCount} className="h-24 text-center text-muted-foreground">
                                {emptyMessage}
                            </TableCell>
                        </TableRow>
                    </TableBody>
                </table>
            </div>
        );
    }

    return (
        // min-w-0 lets the table shrink inside flex/grid parents instead of forcing them to overflow.
        <div className={cn("relative min-w-0", className)}>
            <div ref={tableContainerRef} className="overflow-auto rounded-lg border" style={{ height: heightInPx }}>
                <table className="grid min-w-full caption-bottom text-sm" style={{ width: table.getTotalSize() }}>
                    <VirtualTableHeader table={table} />
                    <TableBody
                        className="relative grid"
                        style={{
                            height: `${rowVirtualizer.getTotalSize()}px`,
                        }}
                    >
                        {rowVirtualizer.getVirtualItems().map((virtualRow) => {
                            const row = rows[virtualRow.index];

                            if (!row) {
                                return null;
                            }

                            return (
                                <TableRow
                                    key={row.id}
                                    data-index={virtualRow.index}
                                    data-state={getRowState?.(row.id)}
                                    ref={(node) => rowVirtualizer.measureElement(node)}
                                    className="absolute flex w-full"
                                    style={{
                                        height: `${virtualRow.size}px`,
                                        transform: `translateY(${virtualRow.start}px)`,
                                    }}
                                >
                                    {row.getVisibleCells().map((cell) => (
                                        <TableCell
                                            key={cell.id}
                                            data-column-id={cell.column.id}
                                            className={cn(
                                                "flex items-center overflow-hidden",
                                                getCellClassName?.(cell.id),
                                            )}
                                            style={getColumnStyle(cell.column.getSize())}
                                        >
                                            <span className="truncate">
                                                {flexRender(cell.column.columnDef.cell, cell.getContext())}
                                            </span>
                                        </TableCell>
                                    ))}
                                </TableRow>
                            );
                        })}
                    </TableBody>
                </table>
            </div>
            {overlay}
        </div>
    );
}

function VirtualTableHeader<TData>({ table }: { table: ReactTable<TData> }) {
    return (
        <TableHeader className="sticky top-0 z-10 grid bg-background">
            {table.getHeaderGroups().map((headerGroup) => (
                <TableRow key={headerGroup.id} className="flex w-full hover:bg-transparent">
                    {headerGroup.headers.map((header) => (
                        <TableHead
                            key={header.id}
                            data-column-id={header.column.id}
                            className="group relative flex items-center overflow-hidden"
                            style={getColumnStyle(header.getSize())}
                        >
                            <span className="truncate">
                                {header.isPlaceholder
                                    ? null
                                    : flexRender(header.column.columnDef.header, header.getContext())}
                            </span>
                            {header.column.getCanResize() && <ColumnResizer header={header} />}
                        </TableHead>
                    ))}
                </TableRow>
            ))}
        </TableHeader>
    );
}

function ColumnResizer<TData>({ header }: { header: Header<TData, unknown> }) {
    return (
        <div
            role="separator"
            aria-orientation="vertical"
            onMouseDown={header.getResizeHandler()}
            onTouchStart={header.getResizeHandler()}
            className={cn(
                "absolute top-0 right-0 h-full w-1 cursor-col-resize touch-none bg-border opacity-0 transition-opacity select-none group-hover:opacity-100",
                header.column.getIsResizing() && "bg-primary opacity-100",
            )}
        />
    );
}
