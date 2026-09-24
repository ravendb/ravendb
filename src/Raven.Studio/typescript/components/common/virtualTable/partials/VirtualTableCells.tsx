import { flexRender, Row } from "@tanstack/react-table";
import classNames from "classnames";

interface VirtualTableCellsProps<T> {
    row: Row<T>;
    isCompact?: boolean;
}

export default function VirtualTableCells<T>({ row, isCompact }: VirtualTableCellsProps<T>) {
    return (
        <>
            {row.getVisibleCells().map((cell) => {
                const isPinned = cell.column.getIsPinned();
                return (
                    <td
                        key={cell.id}
                        style={{
                            width: cell.column.getSize(),
                            padding: isCompact ? "0px 7.5px" : undefined,
                            ...(isPinned
                                ? {
                                      position: "sticky",
                                      left: cell.column.getStart("left"),
                                  }
                                : {}),
                        }}
                        className={classNames("align-content-center", {
                            "col-pinned": isPinned,
                            "font-size-11": isCompact,
                        })}
                    >
                        {flexRender(cell.column.columnDef.cell, cell.getContext())}
                    </td>
                );
            })}
        </>
    );
}
