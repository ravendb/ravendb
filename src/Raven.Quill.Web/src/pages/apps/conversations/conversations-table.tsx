/* eslint-disable react-hooks/incompatible-library */
"use no memo";

import { useMemo } from "react";
import { getCoreRowModel, useReactTable } from "@tanstack/react-table";
import type { ConversationDto } from "@/api/generated/server-api";
import { VirtualDataTable, VirtualDataTableSkeleton } from "@/components/table/virtual-data-table";
import { createConversationColumns } from "@/pages/apps/conversations/conversations-columns";

const ROW_HEIGHT_IN_PX = 60;
const NO_CONVERSATIONS: ConversationDto[] = [];

export function ConversationsTable({
    slug,
    conversations,
    emptyMessage,
}: {
    slug: string;
    conversations: ConversationDto[];
    emptyMessage: string;
}) {
    // react-table (and its row models) want stable references across renders; "use no memo" opts this
    // file out of the React Compiler, so the columns are memoized explicitly. The parent memoizes the
    // filtered `conversations` array.
    const columns = useMemo(() => createConversationColumns(slug), [slug]);

    const table = useReactTable({
        columns,
        data: conversations,
        getCoreRowModel: getCoreRowModel(),
        getRowId: (conversation) => conversation.id,
    });

    return (
        <VirtualDataTable
            table={table}
            columnCount={columns.length}
            emptyMessage={emptyMessage}
            maxHeight={520}
            rowHeightInPx={ROW_HEIGHT_IN_PX}
            className="bg-card"
        />
    );
}

export function ConversationsTableSkeleton({ slug }: { slug: string }) {
    const columns = useMemo(() => createConversationColumns(slug), [slug]);

    const table = useReactTable({
        columns,
        data: NO_CONVERSATIONS,
        getCoreRowModel: getCoreRowModel(),
        getRowId: (conversation) => conversation.id,
    });

    return <VirtualDataTableSkeleton table={table} rows={5} rowHeightInPx={ROW_HEIGHT_IN_PX} className="bg-card" />;
}
