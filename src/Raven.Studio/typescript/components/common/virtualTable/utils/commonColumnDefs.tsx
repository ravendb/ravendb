import { ColumnDef } from "@tanstack/react-table";
import { todo } from "common/developmentHelper";
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
};

todo("Feature", "Damian", "Selecting many rows when holding shift");

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
};
