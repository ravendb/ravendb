import "./VirtualTable.scss";
import { useRef } from "react";
import { flexRender } from "@tanstack/react-table";
import { useVirtualizer } from "@tanstack/react-virtual";
import { ClassNameProps } from "components/models/common";
import VirtualTableBodyWrapper, { VirtualTableBodyWrapperProps } from "./bits/VirtualTableBodyWrapper";
import { virtualTableConstants } from "components/common/virtualTable/utils/virtualTableConstants";

interface VirtualTableProps<T> extends Omit<VirtualTableBodyWrapperProps<T>, "tableContainerRef"> {
    overscan?: number;
}

export default function VirtualTable<T>(props: VirtualTableProps<T> & ClassNameProps) {
    const { table, className, heightInPx = 300, overscan = 5, isLoading = false } = props;

    const tableContainerRef = useRef<HTMLDivElement>(null);
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
                            }}
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
