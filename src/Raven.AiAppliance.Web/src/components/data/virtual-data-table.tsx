import { useRef } from "react";
import { flexRender, type Table as ReactTable } from "@tanstack/react-table";
import { useVirtualizer } from "@tanstack/react-virtual";
import { TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/shadcn/ui/table";
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
    getCellClassName?: (cellId: string) => string | undefined;
    getRowState?: (rowId: string) => string | undefined;
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
}: VirtualDataTableProps<TData>) {
    const tableContainerRef = useRef<HTMLDivElement>(null);
    const rows = table.getRowModel().rows;

    // eslint-disable-next-line react-hooks/incompatible-library
    const rowVirtualizer = useVirtualizer({
        count: rows.length,
        estimateSize: () => rowHeightInPx,
        getScrollElement: () => tableContainerRef.current,
        overscan,
    });

    if (rows.length === 0) {
        return (
            <div className={cn("overflow-hidden rounded-lg border", className)}>
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
        <div className={cn("overflow-hidden rounded-lg border", className)}>
            <div ref={tableContainerRef} className="overflow-auto" style={{ height: heightInPx }}>
                <table className="grid w-full caption-bottom text-sm">
                    <VirtualTableHeader table={table} />
                    <TableBody
                        className="relative grid"
                        style={{
                            height: `${rowVirtualizer.getTotalSize()}px`,
                        }}
                    >
                        {rowVirtualizer.getVirtualItems().map((virtualRow) => {
                            const row = rows[virtualRow.index];

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
                                            className={cn("flex items-center", getCellClassName?.(cell.id))}
                                            style={{
                                                width: cell.column.getSize(),
                                            }}
                                        >
                                            {flexRender(cell.column.columnDef.cell, cell.getContext())}
                                        </TableCell>
                                    ))}
                                </TableRow>
                            );
                        })}
                    </TableBody>
                </table>
            </div>
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
                            className="flex items-center"
                            style={{
                                width: header.getSize(),
                            }}
                        >
                            {header.isPlaceholder
                                ? null
                                : flexRender(header.column.columnDef.header, header.getContext())}
                        </TableHead>
                    ))}
                </TableRow>
            ))}
        </TableHeader>
    );
}
