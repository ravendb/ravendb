import { ColumnDef } from "@tanstack/react-table";
import { Checkbox } from "components/common/Checkbox";
import { CellDocumentPreviewWrapper } from "components/common/virtualTable/cells/CellDocumentPreview";

export const columnPreview: ColumnDef<unknown> = {
    header: "Preview",
    accessorFn: (x) => x,
    cell: CellDocumentPreviewWrapper,
    size: 64,
    minSize: 64,
    enableSorting: false,
    enableHiding: false,
    enableColumnFilter: false,
};

export const columnCheckbox: ColumnDef<unknown> = {
    id: "Checkbox",
    header: ({ table }) => (
        <Checkbox
            selected={table.getIsAllRowsSelected()}
            indeterminate={table.getIsSomeRowsSelected()}
            toggleSelection={(e) => {
                if (table.getIsSomeRowsSelected()) {
                    table.toggleAllRowsSelected(false);
                    return;
                }
                table.toggleAllRowsSelected(e.target.checked);
            }}
            className="selection-checkbox"
        />
    ),
    accessorFn: (x) => x,
    cell: ({ row }) => {
        return (
            <Checkbox
                selected={row.getIsSelected()}
                toggleSelection={row.getToggleSelectedHandler()}
                disabled={!row.getCanSelect()}
                className="selection-checkbox"
            />
        );
    },
    size: 33,
    minSize: 33,
    enableSorting: false,
    enableHiding: false,
    enableColumnFilter: false,
    enablePinning: false,
};

export function createLazySelectionColumn<T>(selectAllLabel: string, selectRowLabel: string): ColumnDef<T> {
    return {
        ...columnCheckbox,
        accessorFn: (x: T) => x,
        header: ({ table }) => {
            const { selectionState, toggleAll } = table.options.meta.lazySelection;

            return (
                <Checkbox
                    selected={selectionState === "AllSelected"}
                    indeterminate={selectionState === "SomeSelected"}
                    toggleSelection={toggleAll}
                    aria-label={selectAllLabel}
                    className="selection-checkbox"
                />
            );
        },
        cell: ({ row, table }) => (
            <Checkbox
                selected={row.getIsSelected()}
                toggleSelection={(e) =>
                    table.options.meta.lazySelection.toggleRow(row.original, (e.nativeEvent as MouseEvent).shiftKey)
                }
                aria-label={selectRowLabel}
                className="selection-checkbox"
            />
        ),
    } as ColumnDef<T>;
}
