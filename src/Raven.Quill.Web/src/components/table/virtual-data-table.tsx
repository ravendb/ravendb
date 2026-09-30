/* eslint-disable react-hooks/incompatible-library */
// https://github.com/TanStack/table/issues/5567
"use no memo";

import { useRef, type CSSProperties, type ReactNode } from "react";
import { flexRender, type Header, type Table as ReactTable } from "@tanstack/react-table";
import { useVirtualizer } from "@tanstack/react-virtual";
import { Skeleton } from "@/components/shadcn/ui/skeleton";
import { TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/shadcn/ui/table";
import { getSkeletonCellWidth } from "@/components/table/skeleton-cell-width";
import { useAutoSizeColumns } from "@/components/table/use-auto-size-columns";
import { cn } from "@/lib/utils";

const DEFAULT_TABLE_MAX_HEIGHT_IN_PX = 360;
const DEFAULT_ROW_HEIGHT_IN_PX = 42;
const DEFAULT_OVERSCAN = 8;
const FILL_MIN_VISIBLE_ROWS = 4;
// Sticky header row (h-10) plus the container borders, on top of the virtualized body height.
const TABLE_CHROME_IN_PX = 42;

interface VirtualDataTableProps<TData> {
    table: ReactTable<TData>;
    columnCount: number;
    emptyMessage: string;
    className?: string;
    maxHeight?: number | "fill";
    overscan?: number;
    rowHeightInPx?: number;
    getCellClassName?: (cellId: string) => string;
    getHeadClassName?: (columnId: string) => string;
    getRowState?: (rowId: string) => string;
    getRowClassName?: (rowId: string) => string;
    /** Called with the row the pointer entered, or null when it left the rows. */
    onRowHoverChange?: (rowId: string | null) => void;
    /** Called with the row that was clicked. Rows are only given a click target when set. */
    onRowClick?: (rowId: string) => void;
    /** Floating content laid over the table region, e.g. a selection toolbar pinned to the bottom edge. */
    overlay?: ReactNode;
}

function getColumnStyle(size: number, { canShrink = false, canGrow = false } = {}): CSSProperties {
    return { width: size, flexGrow: canGrow ? 1 : 0, flexShrink: canShrink ? 1 : 0 };
}

export function VirtualDataTable<TData>({
    table,
    columnCount,
    emptyMessage,
    className,
    maxHeight = DEFAULT_TABLE_MAX_HEIGHT_IN_PX,
    overscan = DEFAULT_OVERSCAN,
    rowHeightInPx = DEFAULT_ROW_HEIGHT_IN_PX,
    getCellClassName,
    getHeadClassName,
    getRowState,
    getRowClassName,
    onRowHoverChange,
    onRowClick,
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
                    <VirtualTableHeader table={table} getHeadClassName={getHeadClassName} canShrinkColumns />
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

    const isFillHeight = maxHeight === "fill";
    // The min-height keeps a short viewport from collapsing the table to a sliver: the parent flex
    // column can shrink the table down to ~4 rows and past that point the page scrolls instead.
    // Tables shorter than the floor stay fully visible; fit-content is pixel-exact there, while the
    // approximate chrome constant only matters for tall tables where a few px are irrelevant.
    const fillFloorInPx = FILL_MIN_VISIBLE_ROWS * rowHeightInPx + TABLE_CHROME_IN_PX;
    const fillMinHeight =
        rowVirtualizer.getTotalSize() <= FILL_MIN_VISIBLE_ROWS * rowHeightInPx ? "fit-content" : fillFloorInPx;

    return (
        // min-w-0 lets the table shrink inside flex/grid parents instead of forcing them to overflow.
        <div
            className={cn("relative min-w-0", isFillHeight && "flex flex-col", className)}
            style={isFillHeight ? { minHeight: fillMinHeight } : undefined}
        >
            {/* maxHeight is a cap, not a fixed height: the body is sized to rowHeightInPx * row count,
                so the container shrinks to fit a short list and only scrolls once it would exceed the cap
                (the pixel value, or in fill mode the space granted by the parent flex column). */}
            <div
                ref={tableContainerRef}
                className={cn("overflow-auto rounded-lg border", isFillHeight && "min-h-0")}
                style={isFillHeight ? undefined : { maxHeight }}
            >
                <table className="grid min-w-full caption-bottom text-sm" style={{ width: table.getTotalSize() }}>
                    <VirtualTableHeader table={table} getHeadClassName={getHeadClassName} />
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
                                    onPointerEnter={() => onRowHoverChange?.(row.id)}
                                    onPointerLeave={() => onRowHoverChange?.(null)}
                                    onClick={onRowClick ? () => onRowClick(row.id) : undefined}
                                    className={cn("absolute flex w-full", getRowClassName?.(row.id))}
                                    // Positioned via top instead of translateY: Chromium never shrinks
                                    // scrollable overflow contributed by transformed children, so rows
                                    // that transiently render lower leave a permanent phantom scrollbar.
                                    style={{
                                        height: `${virtualRow.size}px`,
                                        top: virtualRow.start,
                                    }}
                                >
                                    {row.getVisibleCells().map((cell) => (
                                        <TableCell
                                            key={cell.id}
                                            data-column-id={cell.column.id}
                                            className={cn(
                                                "relative flex items-center overflow-hidden",
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

/**
 * Placeholder for a `VirtualDataTable` that is still loading. Takes the same table instance, so the
 * header labels are the real ones and the placeholder columns line up under them. Content-based
 * auto-sizing needs rows to measure, so until they land the columns start from their configured
 * widths and share out the leftover container width the way auto-sizing will.
 */
export function VirtualDataTableSkeleton<TData>({
    table,
    rows = 5,
    className,
    getHeadClassName,
    getCellClassName,
    rowHeightInPx = DEFAULT_ROW_HEIGHT_IN_PX,
}: {
    table: ReactTable<TData>;
    rows?: number;
    className?: string;
    getHeadClassName?: (columnId: string) => string;
    getCellClassName?: (columnId: string) => string;
    rowHeightInPx?: number;
}) {
    const columns = table.getVisibleLeafColumns();
    const hasActionColumn = columns.at(-1)?.columnDef.header === "";

    return (
        <div className={cn("min-w-0 overflow-hidden rounded-lg border", className)}>
            <table className="w-full caption-bottom text-sm">
                <VirtualTableHeader table={table} getHeadClassName={getHeadClassName} canShrinkColumns canGrowColumns />
                <TableBody className="grid">
                    {Array.from({ length: rows }).map((_, rowIndex) => (
                        <TableRow
                            key={rowIndex}
                            className="flex w-full hover:bg-transparent"
                            style={{ height: rowHeightInPx }}
                        >
                            {columns.map((column, columnIndex) => (
                                <TableCell
                                    key={column.id}
                                    className={cn("flex items-center overflow-hidden", getCellClassName?.(column.id))}
                                    style={getColumnStyle(column.getSize(), {
                                        canShrink: column.getCanResize(),
                                        canGrow: column.getCanResize(),
                                    })}
                                >
                                    <Skeleton
                                        className={cn(
                                            "h-4",
                                            getSkeletonCellWidth(columnIndex, columns.length, hasActionColumn),
                                        )}
                                    />
                                </TableCell>
                            ))}
                        </TableRow>
                    ))}
                </TableBody>
            </table>
        </div>
    );
}

function VirtualTableHeader<TData>({
    table,
    getHeadClassName,
    canShrinkColumns = false,
    canGrowColumns = false,
}: {
    table: ReactTable<TData>;
    getHeadClassName?: (columnId: string) => string;
    canShrinkColumns?: boolean;
    canGrowColumns?: boolean;
}) {
    return (
        <TableHeader className="sticky top-0 z-10 grid grid-cols-1 bg-background">
            {table.getHeaderGroups().map((headerGroup) => (
                <TableRow key={headerGroup.id} className="flex w-full hover:bg-transparent">
                    {headerGroup.headers.map((header) => (
                        <TableHead
                            key={header.id}
                            data-column-id={header.column.id}
                            className={cn(
                                "group relative flex items-center overflow-hidden",
                                getHeadClassName?.(header.column.id),
                            )}
                            style={getColumnStyle(header.getSize(), {
                                canShrink: canShrinkColumns && header.column.getCanResize(),
                                canGrow: canGrowColumns && header.column.getCanResize(),
                            })}
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
