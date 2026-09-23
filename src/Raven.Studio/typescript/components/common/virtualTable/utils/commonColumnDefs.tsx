import { ColumnDef } from "@tanstack/react-table";
import { Checkbox } from "components/common/Checkbox";
import { CellDocumentPreviewWrapper } from "components/common/virtualTable/cells/CellDocumentPreview";
import { ChangeEvent } from "react";

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

// TODO Selecting many rows when holding shift

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
        />
    ),
    accessorFn: (x) => x,
    cell: ({ row }) => {
        return (
            <Checkbox
                selected={row.getIsSelected()}
                toggleSelection={row.getToggleSelectedHandler()}
                disabled={!row.getCanSelect()}
            />
        );
    },
    size: 38,
    minSize: 38,
    enableSorting: false,
    enableHiding: false,
    enableColumnFilter: false,
    enablePinning: false,
};

interface LazySelectionColumnLabels {
    selectAllLabel: string;
    selectRowLabel: string;
}

// reads the selection from the table meta (lazySelection) when clicked, so memoized rows never use a stale one
export function createLazySelectionColumn<T>({
    selectAllLabel,
    selectRowLabel,
}: LazySelectionColumnLabels): ColumnDef<T> {
    return {
        id: columnCheckbox.id,
        accessorFn: (x) => x,
        size: columnCheckbox.size,
        minSize: columnCheckbox.minSize,
        enableSorting: false,
        enableHiding: false,
        enableColumnFilter: false,
        enablePinning: false,
        header: ({ table }) => {
            const { selectionState, toggleAll } = table.options.meta.lazySelection;

            return (
                <Checkbox
                    selected={selectionState === "AllSelected"}
                    indeterminate={selectionState === "SomeSelected"}
                    toggleSelection={toggleAll}
                    aria-label={selectAllLabel}
                />
            );
        },
        cell: ({ row, table }) => (
            <Checkbox
                selected={row.getIsSelected()}
                toggleSelection={(e) => table.options.meta.lazySelection.toggleRow(row.original, isShiftKeyPressed(e))}
                aria-label={selectRowLabel}
            />
        ),
    };
}

// React fires the change event of a checkbox from the click, so the mouse modifiers are available
function isShiftKeyPressed(e: ChangeEvent<HTMLInputElement>) {
    return e.nativeEvent instanceof MouseEvent && e.nativeEvent.shiftKey;
}
