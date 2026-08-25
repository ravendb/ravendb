/* eslint-disable react-hooks/incompatible-library */
"use no memo";

import { useMemo } from "react";
import { useQuery } from "@tanstack/react-query";
import { getCoreRowModel, useReactTable } from "@tanstack/react-table";
import { api } from "@/api/api";
import { ApiState } from "@/components/data/api-state";
import { VirtualDataTable, VirtualDataTableSkeleton } from "@/components/table/virtual-data-table";
import { createActiveLinkColumns } from "@/pages/apps/channels/channel-active-links-columns";

export function ChannelActiveLinks({ slug, channelId }: { slug: string; channelId: string }) {
    const linksQuery = useQuery(api.queries.embedLinks.list(slug));

    // react-table (and its row models) want stable references across renders; "use no memo" opts this
    // file out of the React Compiler, so the data and columns are memoized explicitly.
    const links = useMemo(
        () => (linksQuery.data ?? []).filter((link) => link.channelId === channelId),
        [linksQuery.data, channelId],
    );
    const columns = useMemo(() => createActiveLinkColumns(slug), [slug]);

    const table = useReactTable({
        columns,
        data: links,
        getCoreRowModel: getCoreRowModel(),
        getRowId: (link) => link.token,
    });

    return (
        <ApiState
            isLoading={linksQuery.isPending}
            isError={linksQuery.isError}
            errorTitle="Could not load links"
            onRetry={() => void linksQuery.refetch()}
            loadingLabel="Loading links..."
            skeleton={<VirtualDataTableSkeleton table={table} rows={3} className="bg-card" />}
        >
            <VirtualDataTable
                table={table}
                columnCount={columns.length}
                emptyMessage="No active links. Generate one to embed this agent for a specific user."
                className="bg-card"
            />
        </ApiState>
    );
}
