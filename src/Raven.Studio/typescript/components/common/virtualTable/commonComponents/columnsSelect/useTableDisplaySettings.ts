import { Column, Table as TanstackTable } from "@tanstack/react-table";
import { CustomColumnDefinition } from "components/common/virtualTable/commonComponents/columnsSelect/customColumns";
import { columnCheckbox } from "components/common/virtualTable/utils/commonColumnDefs";

export interface ColumnMeta {
    id: string;
    headerTitle: string;
    canHide: boolean;
    canPin: boolean;
    // set for the columns defined by the user, they can be edited and removed
    customColumn?: CustomColumnDefinition;
}

function getColumnHeaderTitle<T>(column: Column<T, unknown>): string {
    const { columnDef } = column;
    if (typeof columnDef.header === "string") {
        return columnDef.header;
    }
    if ("accessorKey" in columnDef && typeof columnDef.accessorKey === "string") {
        return columnDef.accessorKey;
    }
    return columnDef.id ?? column.id;
}

export function useTableDisplaySettings<T>(table: TanstackTable<T>) {
    const tableColumns = table.getAllColumns();
    const fixedLeadingColumnIds = tableColumns.map((x) => x.id).filter((id) => id === columnCheckbox.id);
    const allColumns = tableColumns.filter((column) => !fixedLeadingColumnIds.includes(column.id));

    const columnMetas: ColumnMeta[] = allColumns.map((column) => ({
        id: column.id,
        headerTitle: getColumnHeaderTitle(column),
        canHide: column.getCanHide(),
        canPin: column.getCanPin(),
        customColumn: column.columnDef.meta?.customColumn,
    }));

    const allColumnIds = allColumns.map((x) => x.id);

    const getInitialColumnOrder = (): string[] => {
        const order = table.getState().columnOrder?.filter((id) => allColumnIds.includes(id));
        if (order && order.length > 0) {
            // Fill in any columns not yet in the order (e.g. first open)
            const missing = allColumnIds.filter((id) => !order.includes(id));
            return [...order, ...missing];
        }
        return allColumnIds;
    };

    const getInitialPinnedIds = (): string[] => {
        const pinning = table.getState().columnPinning;
        return (pinning?.left ?? []).filter((id) => allColumnIds.includes(id));
    };

    const getInitialSelectedIds = (): string[] => allColumns.filter((column) => column.getIsVisible()).map((x) => x.id);

    const applySettings = (selectedIds: string[], columnOrder: string[], pinnedIds: string[]) => {
        allColumns.forEach((column) => {
            if (column.getCanHide()) {
                column.toggleVisibility(selectedIds.includes(column.id));
            }
        });
        table.setColumnOrder([...fixedLeadingColumnIds, ...columnOrder]);
        table.setColumnPinning({ left: [...fixedLeadingColumnIds, ...pinnedIds] });
    };

    return {
        columnMetas,
        allColumnIds,
        getInitialColumnOrder,
        getInitialPinnedIds,
        getInitialSelectedIds,
        applySettings,
    };
}
