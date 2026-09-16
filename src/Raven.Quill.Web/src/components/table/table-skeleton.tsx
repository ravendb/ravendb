import { Skeleton } from "@/components/shadcn/ui/skeleton";
import { TableCell, TableRow } from "@/components/shadcn/ui/table";
import { getSkeletonCellWidth } from "@/components/table/skeleton-cell-width";
import { cn } from "@/lib/utils";

/**
 * Placeholder body rows for a table that is still loading. Goes inside the same table the loaded
 * rows will fill, so the header row is the real one and the column count cannot drift.
 */
export function TableSkeletonRows({
    columnCount,
    rows = 5,
    hasActionColumn = false,
}: {
    columnCount: number;
    rows?: number;
    hasActionColumn?: boolean;
}) {
    return Array.from({ length: rows }).map((_, rowIndex) => (
        <TableRow key={rowIndex} className="hover:bg-transparent">
            {Array.from({ length: columnCount }).map((_, columnIndex) => {
                const isActionCell = hasActionColumn && columnIndex === columnCount - 1;
                return (
                    <TableCell key={columnIndex}>
                        <Skeleton
                            className={cn(
                                "h-4",
                                getSkeletonCellWidth(columnIndex, columnCount, hasActionColumn),
                                // Loaded action cells sit against the right edge under a right-aligned head.
                                isActionCell && "ml-auto",
                            )}
                        />
                    </TableCell>
                );
            })}
        </TableRow>
    ));
}
