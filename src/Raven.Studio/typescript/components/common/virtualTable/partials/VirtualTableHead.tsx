import { Column, flexRender, Table as TanstackTable } from "@tanstack/react-table";
import classNames from "classnames";
import "./VirtualTableHead.scss";
import ColumnSettings from "components/common/virtualTable/partials/VirtualTableColumnSettings";
import { virtualTableConstants } from "../utils/virtualTableConstants";

interface VirtualTableHeadProps<T> {
    table: TanstackTable<T>;
    isCompact?: boolean;
}

export default function VirtualTableHead<T>({ table, isCompact }: VirtualTableHeadProps<T>) {
    const height = isCompact ? virtualTableConstants.compactHeaderHeightInPx : virtualTableConstants.headerHeightInPx;

    return (
        <thead
            style={{
                height,
            }}
        >
            {table.getHeaderGroups().map((headerGroup) => (
                <tr key={headerGroup.id} className="d-flex">
                    {headerGroup.headers.map((header) => {
                        const isPinned = header.column.getIsPinned();

                        return (
                            <th
                                key={header.id}
                                className={classNames("position-relative align-content-center", {
                                    "font-size-11 line-height-25 py-0 px-1": isCompact,
                                    "col-pinned": isPinned,
                                })}
                                style={{
                                    width: header.getSize(),
                                    height,
                                    ...(isPinned ? { position: "sticky", left: header.column.getStart("left") } : {}),
                                }}
                            >
                                <div
                                    className="position-relative d-flex align-items-center justify-content-between"
                                    title={getHeaderTitle(header.column)}
                                >
                                    <span className="text-truncate w-100">
                                        {flexRender(header.column.columnDef.header, header.getContext())}
                                    </span>

                                    <ColumnSettings column={header.column} isCompact={isCompact} />
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
                        );
                    })}
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
