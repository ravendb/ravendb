import { getCoreRowModel, getFilteredRowModel, getSortedRowModel, useReactTable } from "@tanstack/react-table";

export default function useIndexErrorsTable() {
    const indexErrorsPanelTable = useReactTable({
        data: [],
        columns: [],
        columnResizeMode: "onChange",
        getCoreRowModel: getCoreRowModel(),
        getSortedRowModel: getSortedRowModel(),
        getFilteredRowModel: getFilteredRowModel(),
    });

    return { indexErrorsPanelTable };
}
