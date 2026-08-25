/* eslint-disable react-hooks/incompatible-library */
"use no memo";

import { useMemo } from "react";
import { useQuery } from "@tanstack/react-query";
import { getCoreRowModel, useReactTable, type ColumnDef } from "@tanstack/react-table";
import { api } from "@/api/api";
import type { DataCollectionDto } from "@/api/generated/server-api";
import { ApiState } from "@/components/data/api-state";
import { Badge } from "@/components/shadcn/ui/badge";
import { VirtualDataTable, VirtualDataTableSkeleton } from "@/components/table/virtual-data-table";
import { formatCompact } from "@/lib/format";
import { SectionCard } from "@/pages/apps/section-card";

const collectionColumns: ColumnDef<DataCollectionDto>[] = [
    {
        accessorKey: "name",
        header: "Collection",
        cell: ({ getValue }) => <span className="font-medium">{getValue<string>()}</span>,
    },
    {
        accessorKey: "documentsCount",
        header: "Documents",
        cell: ({ getValue }) => <span className="tabular-nums">{formatCompact(getValue<number>())}</span>,
    },
];

export function CollectionsSection({ slug }: { slug: string }) {
    const collectionsQuery = useQuery(api.queries.stats.collections(slug));

    // react-table (and its row models) want stable references across renders; "use no memo" opts this
    // file out of the React Compiler, so the data is memoized explicitly. The columns are static.
    const collections = useMemo(() => collectionsQuery.data ?? [], [collectionsQuery.data]);

    const table = useReactTable({
        columns: collectionColumns,
        data: collections,
        getCoreRowModel: getCoreRowModel(),
        getRowId: (collection) => collection.name,
    });

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
                skeleton={<VirtualDataTableSkeleton table={table} rows={4} className="bg-card" />}
            >
                <VirtualDataTable
                    table={table}
                    columnCount={collectionColumns.length}
                    emptyMessage="No collections yet."
                    className="bg-card"
                />
            </ApiState>
        </SectionCard>
    );
}
