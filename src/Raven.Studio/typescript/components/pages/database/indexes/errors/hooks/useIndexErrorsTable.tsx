import {
    ColumnDef,
    getCoreRowModel,
    getFilteredRowModel,
    getSortedRowModel,
    useReactTable,
} from "@tanstack/react-table";

const EMPTY_DATA: IndexErrorPerDocument[] = [];
const EMPTY_COLUMNS: ColumnDef<IndexErrorPerDocument>[] = [];

export default function useIndexErrorsTable() {
    const indexErrorsPanelTable = useReactTable<IndexErrorPerDocument>({
        data: EMPTY_DATA,
        columns: EMPTY_COLUMNS,
        columnResizeMode: "onChange",
        getCoreRowModel: getCoreRowModel(),
        getSortedRowModel: getSortedRowModel(),
        getFilteredRowModel: getFilteredRowModel(),
    });

    return { indexErrorsPanelTable };
}
