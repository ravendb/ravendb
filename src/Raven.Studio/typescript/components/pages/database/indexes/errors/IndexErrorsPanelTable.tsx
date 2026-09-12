import { useIndexErrorsPanelColumns } from "components/pages/database/indexes/errors/hooks/useIndexErrorsPanelColumns";
import { Table, useReactTable } from "@tanstack/react-table";
import { LoadError } from "components/common/LoadError";
import VirtualTable from "components/common/virtualTable/VirtualTable";
import { AsyncStateStatus } from "react-async-hook";
import { useMemo } from "react";

interface IndexErrorsPanelTableProps {
    status: AsyncStateStatus;
    refresh: () => void;
    indexErrors: IndexErrorPerDocument[] | undefined;
    width: number;
    isLoading: boolean;
    table: Table<IndexErrorPerDocument>;
    // when reused for a debug-package snapshot the edit-index / view-document hyperlinks point at the
    // live server and are meaningless, so callers can render plain values instead
    disableLinks?: boolean;
}

export function IndexErrorsPanelTable({
    status,
    isLoading,
    refresh,
    indexErrors,
    width,
    table,
    disableLinks,
}: IndexErrorsPanelTableProps) {
    const { indexErrorsPanelColumns } = useIndexErrorsPanelColumns(width);

    const data = useMemo(() => indexErrors ?? [], [indexErrors]);

    const indexErrorsPanelTable = useReactTable<IndexErrorPerDocument>({
        ...table.options,
        data,
        columns: indexErrorsPanelColumns,
        meta: { ...table.options.meta, disableLinks },
    });

    return (
        <>
            {status === "error" ? (
                <LoadError error="Error during loading identities" refresh={refresh} />
            ) : (
                <VirtualTable heightInPx={400} table={indexErrorsPanelTable} isLoading={isLoading} />
            )}
        </>
    );
}
