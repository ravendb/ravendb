import type { ReactNode } from "react";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/shadcn/ui/table";

export function SectionCard({ title, action, children }: { title: string; action?: ReactNode; children: ReactNode }) {
    return (
        <section>
            <div className="mb-4 flex items-center justify-between gap-3">
                <h2 className="text-sm font-semibold">{title}</h2>
                {action}
            </div>
            {children}
        </section>
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
        <div className="overflow-hidden rounded-lg border">
            <Table>
                <TableHeader>
                    <TableRow className="hover:bg-transparent">
                        {headers.map((header) => (
                            <TableHead key={header} className="text-xs font-medium text-muted-foreground">
                                {header}
                            </TableHead>
                        ))}
                    </TableRow>
                </TableHeader>
                <TableBody>
                    {isEmpty ? (
                        <TableRow className="hover:bg-transparent">
                            <TableCell colSpan={headers.length} className="h-20 text-center text-muted-foreground">
                                {emptyMessage}
                            </TableCell>
                        </TableRow>
                    ) : (
                        children
                    )}
                </TableBody>
            </Table>
        </div>
    );
}
