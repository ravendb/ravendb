import React from "react";
import VirtualTable from "components/common/virtualTable/VirtualTable";
import { getCoreRowModel, getFilteredRowModel, getSortedRowModel, useReactTable } from "@tanstack/react-table";
import {
    ServerSettingsColumns,
    useServerSettingsColumns,
} from "components/pages/resources/manageServer/serverSettings/useServerSettingsColumns";
import { AsyncStateStatus } from "react-async-hook";
import { LoadError } from "components/common/LoadError";

interface ServerSettingsVirtualGridProps {
    height: number;
    width: number;
    data: ServerSettingsColumns[];
    isLoading: boolean;
    status: AsyncStateStatus;
    reload: () => void;
}

export function ServerSettingsVirtualTable({
    height,
    data,
    isLoading,
    reload,
    width,
    status,
}: ServerSettingsVirtualGridProps) {
    const serverSettingsColumns = useServerSettingsColumns(width);

    const serverSettingsTable = useReactTable({
        columnResizeMode: "onChange",
        columns: serverSettingsColumns,
        data,
        getCoreRowModel: getCoreRowModel(),
        getFilteredRowModel: getFilteredRowModel(),
        getSortedRowModel: getSortedRowModel(),
        initialState: {
            sorting: [
                {
                    id: "origin",
                    desc: true,
                },
            ],
        },
    });

    if (status === "error") {
        return <LoadError error="Error during loading server settings" refresh={() => reload()} />;
    }

    return <VirtualTable className="mt-4" isLoading={isLoading} heightInPx={height} table={serverSettingsTable} />;
}
