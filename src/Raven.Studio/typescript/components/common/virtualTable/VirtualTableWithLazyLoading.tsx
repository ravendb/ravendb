import "./VirtualTable.scss";
import { flexRender } from "@tanstack/react-table";
import { ClassNameProps } from "components/models/common";
import { virtualTableConstants } from "components/common/virtualTable/utils/virtualTableConstants";
import VirtualTableBodyWrapper, { VirtualTableBodyWrapperProps } from "./bits/VirtualTableBodyWrapper";

export interface VirtualTableWithLazyLoadingProps<T> extends VirtualTableBodyWrapperProps<T> {
    bodyHeightInPx: number;
    getRowPositionY: (index: number) => number;
}

export default function VirtualTableWithLazyLoading<T>(props: VirtualTableWithLazyLoadingProps<T> & ClassNameProps) {
    const { tableContainerRef, table, className, heightInPx, isLoading, bodyHeightInPx, getRowPositionY } = props;

    // Disable sorting by default for lazy loading
    table.setOptions((prev) => ({
        ...prev,
        defaultColumn: {
            ...prev.defaultColumn,
            enableSorting: false,
        },
    }));

    return (
        <VirtualTableBodyWrapper
            table={table}
            className={className}
            tableContainerRef={tableContainerRef}
            isLoading={isLoading}
            heightInPx={heightInPx}
        >
            <tbody style={{ height: bodyHeightInPx }}>
                {table.getRowModel().rows.map((row, i) => {
                    const positionY = getRowPositionY(i);

                    return (
                        <tr
                            key={i}
                            style={{
                                height: virtualTableConstants.defaultRowHeightInPx,
                                transform: `translateY(${positionY}px)`,
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
