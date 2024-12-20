import { Column, Table as TanstackTable, flexRender } from "@tanstack/react-table";
import classNames from "classnames";
import "./VirtualTableHead.scss";
import ColumnSettings from "components/common/virtualTable/partials/VirtualTableColumnSettings";

interface VirtualTableHeadProps<T> {
    table: TanstackTable<T>;
}

export default function VirtualTableHead<T>({ table }: VirtualTableHeadProps<T>) {
    return (
        <thead>
            {table.getHeaderGroups().map((headerGroup) => (
                <tr key={headerGroup.id} className="d-flex">
                    {headerGroup.headers.map((header) => (
                        <th
                            key={header.id}
                            className="position-relative align-content-center"
                            style={{ width: header.getSize() }}
                        >
                            <div
                                className="position-relative d-flex align-items-center justify-content-between"
                                title={getHeaderTitle(header.column)}
                            >
                                {flexRender(header.column.columnDef.header, header.getContext())}

                                <ColumnSettings column={header.column} />
                            </div>
                            {header.column.getCanResize() && (
                                <div
                                    className={classNames("resizer", {
                                        "is-resizing": header.column.getIsResizing(),
                                    })}
                                    onMouseDown={header.getResizeHandler()}
                                    onTouchStart={header.getResizeHandler()}
                                ></div>
                            )}
                        </th>
                    ))}
                </tr>
            ))}
        </thead>
    );
}

function getHeaderTitle<T>(column: Column<T, unknown>): string {
    const { columnDef } = column;

    if (typeof columnDef.header === "string") {
        return columnDef.header;
    }

    if ("accessorKey" in columnDef && typeof columnDef.accessorKey === "string") {
        return columnDef.accessorKey;
    }

    return columnDef.id;
}
