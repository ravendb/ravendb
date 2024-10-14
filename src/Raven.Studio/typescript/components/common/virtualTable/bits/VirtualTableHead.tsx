import { Column, Table as TanstackTable, flexRender } from "@tanstack/react-table";
import classNames from "classnames";

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
                            onClick={header.column.getToggleSortingHandler()}
                        >
                            <div
                                onClick={header.column.getToggleSortingHandler()}
                                className={classNames("position-relative", {
                                    "cursor-pointer select-none": header.column.getCanSort(),
                                })}
                                title={getHeaderTitle(header.column)}
                            >
                                {flexRender(header.column.columnDef.header, header.getContext())}

                                {header.column.getCanSort() && (
                                    <div
                                        className={`sortable-controls ${
                                            header.column.getIsSorted() === "asc"
                                                ? "asc"
                                                : header.column.getIsSorted() === "desc"
                                                  ? "desc"
                                                  : ""
                                        }`}
                                    ></div>
                                )}
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

    const getTitle = (): string => {
        if (typeof columnDef.header === "string") {
            return columnDef.header;
        }
        if ("accessorKey" in columnDef && typeof columnDef.accessorKey === "string") {
            return columnDef.accessorKey;
        }
        return columnDef.id;
    };

    if (column.getCanSort()) {
        return `Sort by ${getTitle()}`;
    }

    return getTitle();
}
