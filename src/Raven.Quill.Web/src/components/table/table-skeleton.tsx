import { Skeleton } from "@/components/shadcn/ui/skeleton";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/shadcn/ui/table";
import { cn } from "@/lib/utils";

// Content in a real column is a consistent width, so varying the placeholder by column rather
// than by cell is what makes it read as a table instead of a block of stripes.
const COLUMN_WIDTHS = ["w-32", "w-16", "w-24", "w-20", "w-28", "w-14"];

// Headers are passed through verbatim, so an unlabelled trailing column is an action column.
const ACTION_COLUMN_WIDTH = "w-8";

/**
 * Placeholder for a table that is still loading. Takes the same `headers` the loaded table
 * renders, so the real header row stays on screen and the column count cannot drift.
 */
export function TableSkeleton({ headers, rows = 5 }: { headers: string[]; rows?: number }) {
    return (
        <div className="overflow-hidden rounded-lg border">
            <Table>
                <TableHeader>
                    <TableRow className="hover:bg-transparent">
                        {headers.map((header, index) => (
                            <TableHead key={index} className="text-xs font-medium text-muted-foreground">
                                {header}
                            </TableHead>
                        ))}
                    </TableRow>
                </TableHeader>
                <TableBody>
                    {Array.from({ length: rows }).map((_, rowIndex) => (
                        <TableRow key={rowIndex} className="hover:bg-transparent">
                            {headers.map((header, columnIndex) => (
                                <TableCell key={columnIndex}>
                                    <Skeleton
                                        className={cn(
                                            "h-4",
                                            header
                                                ? COLUMN_WIDTHS[columnIndex % COLUMN_WIDTHS.length]
                                                : ACTION_COLUMN_WIDTH,
                                        )}
                                    />
                                </TableCell>
                            ))}
                        </TableRow>
                    ))}
                </TableBody>
            </Table>
        </div>
    );
}
