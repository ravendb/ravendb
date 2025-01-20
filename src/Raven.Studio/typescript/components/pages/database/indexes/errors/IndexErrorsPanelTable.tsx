import { useIndexErrorsPanelColumns } from "components/pages/database/indexes/errors/hooks/useIndexErrorsPanelColumns";
import { Table, useReactTable } from "@tanstack/react-table";
import { LoadError } from "components/common/LoadError";
import VirtualTable from "components/common/virtualTable/VirtualTable";
import { AsyncStateStatus } from "react-async-hook";

interface IndexErrorsPanelTableProps {
    status: AsyncStateStatus;
    refresh: () => void;
    indexErrors: IndexErrorPerDocument[] | undefined;
    width: number;
    isLoading: boolean;
    table: Table<IndexErrorPerDocument>;
}

export function IndexErrorsPanelTable({
    status,
    isLoading,
    refresh,
    indexErrors,
    width,
    table,
}: IndexErrorsPanelTableProps) {
    const { indexErrorsPanelColumns } = useIndexErrorsPanelColumns(width);

    const indexErrorsPanelTable = useReactTable<IndexErrorPerDocument>({
        ...table.options,
        data: indexErrors ?? [],
        columns: indexErrorsPanelColumns,
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
