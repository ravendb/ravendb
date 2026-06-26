import { useQuery } from "@tanstack/react-query";
import { api } from "@/api/api";
import { ApiState } from "@/components/data/api-state";
import { Badge } from "@/components/shadcn/ui/badge";
import { TableCell, TableRow } from "@/components/shadcn/ui/table";
import { formatCompact } from "@/lib/format";
import { SectionCard, SectionTable } from "@/pages/apps/section-card";

export function CollectionsSection({ slug }: { slug: string }) {
    const collectionsQuery = useQuery(api.queries.stats.collections(slug));

    return (
        <SectionCard
            title="Collections"
            action={
                collectionsQuery.data && (
                    <Badge variant="secondary" className="font-mono">
                        {collectionsQuery.data.length}
                    </Badge>
                )
            }
        >
            <ApiState
                isLoading={collectionsQuery.isPending}
                isError={collectionsQuery.isError}
                errorTitle="Could not load collections"
                onRetry={() => void collectionsQuery.refetch()}
                loadingLabel="Loading collections..."
            >
                {collectionsQuery.data && (
                    <SectionTable
                        headers={["Collection", "Documents", "Fields"]}
                        isEmpty={collectionsQuery.data.length === 0}
                        emptyMessage="No collections yet."
                    >
                        {collectionsQuery.data.map((collection) => (
                            <TableRow key={collection.name}>
                                <TableCell className="font-medium">{collection.name}</TableCell>
                                <TableCell className="tabular-nums">
                                    {formatCompact(collection.documentsCount)}
                                </TableCell>
                                <TableCell className="text-muted-foreground tabular-nums">
                                    {collection.fields.length}
                                </TableCell>
                            </TableRow>
                        ))}
                    </SectionTable>
                )}
            </ApiState>
        </SectionCard>
    );
}
