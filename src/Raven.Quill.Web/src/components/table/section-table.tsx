import type { ReactNode } from "react";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/shadcn/ui/table";
import { TableSkeletonRows } from "@/components/table/table-skeleton";

function SectionTableFrame({ headers, children }: { headers: string[]; children: ReactNode }) {
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
                <TableBody>{children}</TableBody>
            </Table>
        </div>
    );
}

export function SectionTable({
    headers,
    isEmpty,
    emptyMessage,
    children,
}: {
    headers: string[];
    isEmpty: boolean;
    emptyMessage: string;
    children: ReactNode;
}) {
    return (
        <SectionTableFrame headers={headers}>
            {isEmpty ? (
                <TableRow className="hover:bg-transparent">
                    <TableCell colSpan={headers.length} className="h-20 text-center text-muted-foreground">
                        {emptyMessage}
                    </TableCell>
                </TableRow>
            ) : (
                children
            )}
        </SectionTableFrame>
    );
}

/** Placeholder for a `SectionTable` that is still loading, drawn with the same header row. */
export function SectionTableSkeleton({ headers, rows }: { headers: string[]; rows?: number }) {
    return (
        <SectionTableFrame headers={headers}>
            <TableSkeletonRows columnCount={headers.length} rows={rows} hasActionColumn={headers.at(-1) === ""} />
        </SectionTableFrame>
    );
}
