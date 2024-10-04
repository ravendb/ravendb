import "./VirtualTable.scss";
import { useRef } from "react";
import { flexRender } from "@tanstack/react-table";
import { useVirtualizer } from "@tanstack/react-virtual";
import { ClassNameProps } from "components/models/common";
import VirtualTableBodyWrapper, { VirtualTableBodyWrapperProps } from "./bits/VirtualTableBodyWrapper";
import { virtualTableConstants } from "components/common/virtualTable/utils/virtualTableConstants";
import classNames from "classnames";

// Chrome/Edge can render up to 838 859 rows in a table
// Firefox only up to 223 695 rows
// If you want to render more rows, you need to use VirtualTableWithLazyLoading component along with useVirtualTableWithLazyLoading hook

// May have performance problems but only in dev mode (prod build works fine)

interface VirtualTableProps<T> extends Omit<VirtualTableBodyWrapperProps<T>, "tableContainerRef"> {
    overscan?: number;
    tableContainerRef?: React.RefObject<HTMLDivElement>;
}

export default function VirtualTable<T>(props: VirtualTableProps<T> & ClassNameProps) {
    const { table, className, heightInPx = 300, overscan = 5, isLoading = false } = props;

    const innerTableContainerRef = useRef<HTMLDivElement>(null);
    const tableContainerRef = props.tableContainerRef ?? innerTableContainerRef;

    const { rows } = table.getRowModel();

    const rowVirtualizer = useVirtualizer({
        count: rows.length,
        estimateSize: () => virtualTableConstants.defaultRowHeightInPx,
        getScrollElement: () => tableContainerRef.current,
        overscan,
    });

    return (
        <VirtualTableBodyWrapper
            table={table}
            className={className}
            tableContainerRef={tableContainerRef}
            isLoading={isLoading}
            heightInPx={heightInPx}
        >
            <tbody
                style={{
                    height: `${rowVirtualizer.getTotalSize()}px`,
                }}
            >
                {rowVirtualizer.getVirtualItems().map((virtualRow) => {
                    const row = rows[virtualRow.index];
                    return (
                        <tr
                            data-index={virtualRow.index}
                            ref={(node) => rowVirtualizer.measureElement(node)}
                            key={row.id}
                            style={{
                                transform: `translateY(${virtualRow.start}px)`,
                                height: `${virtualRow.size}px`,
                            }}
                            className={classNames({ "is-odd": virtualRow.index % 2 !== 0 })}
                        >
                            {row.getVisibleCells().map((cell) => (
                                <td
                                    key={cell.id}
                                    style={{
                                        width: cell.column.getSize(),
                                    }}
                                    className="align-content-center"
                                >
                                    {flexRender(cell.column.columnDef.cell, cell.getContext())}
                                </td>
                            ))}
                        </tr>
                    );
                })}
            </tbody>
        </VirtualTableBodyWrapper>
    );
}
