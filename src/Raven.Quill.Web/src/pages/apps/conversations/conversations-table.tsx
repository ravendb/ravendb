/* eslint-disable react-hooks/incompatible-library */
"use no memo";

import { useMemo } from "react";
import { getCoreRowModel, useReactTable } from "@tanstack/react-table";
import type { ConversationDto } from "@/api/generated/server-api";
import { VirtualDataTable } from "@/components/table/virtual-data-table";
import { createConversationColumns } from "@/pages/apps/conversations/conversations-columns";

export function ConversationsTable({ slug, conversations }: { slug: string; conversations: ConversationDto[] }) {
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
            emptyMessage="No conversations match the current filters."
            heightInPx={520}
            rowHeightInPx={60}
            className="bg-card"
        />
    );
}
