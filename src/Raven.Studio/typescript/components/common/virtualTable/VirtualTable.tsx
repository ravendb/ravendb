import "./VirtualTable.scss";
import { useRef } from "react";
import { FilterFn, flexRender, Row } from "@tanstack/react-table";
import { useVirtualizer } from "@tanstack/react-virtual";
import { ClassNameProps } from "components/models/common";
import VirtualTableBodyWrapper, { VirtualTableBodyWrapperProps } from "./partials/VirtualTableBodyWrapper";
import { virtualTableConstants } from "components/common/virtualTable/utils/virtualTableConstants";
import classNames from "classnames";
import { values } from "lodash";

// Chrome/Edge can render up to 838 859 rows in a table
// Firefox only up to 223 695 rows
// If you want to render more rows, you need to use VirtualTableWithLazyLoading component along with useVirtualTableWithLazyLoading hook

// May have performance problems but only in dev mode (prod build works fine)

interface VirtualTableProps<T> extends Omit<VirtualTableBodyWrapperProps<T>, "tableContainerRef"> {
    overscan?: number;
    rowHeightInPx?: number;
    tableContainerRef?: React.RefObject<HTMLDivElement>;
    onRowClick?: (row: Row<T>) => void;
    getRowClassName?: (row: Row<T>) => string;
}

export default function VirtualTable<T>(props: VirtualTableProps<T> & ClassNameProps) {
    const {
        table,
        className,
        heightInPx = 300,
        overscan = 5,
        rowHeightInPx,
        isLoading = false,
        isCompact,
        isRoundingDisabled,
        isPaddingDisabled,
        onRowClick,
        getRowClassName,
    } = props;

    const innerTableContainerRef = useRef<HTMLDivElement>(null);
    const tableContainerRef = props.tableContainerRef ?? innerTableContainerRef;

    // Set default filter function
    table.setOptions((prev) => ({
        ...prev,
        columnResizeMode: "onChange",
        defaultColumn: {
            filterFn: "stringifyIncludes" as any, // custom filter function
            ...prev.defaultColumn,
        },
        filterFns: {
            stringifyIncludes: stringifyIncludesFilter,
            ...prev.filterFns,
        },
    }));

    const { rows } = table.getRowModel();

    const rowVirtualizer = useVirtualizer({
        count: rows.length,
        estimateSize: () =>
            rowHeightInPx ??
            (isCompact ? virtualTableConstants.compactRowHeightInPx : virtualTableConstants.defaultRowHeightInPx),
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
            isCompact={isCompact}
            isRoundingDisabled={isRoundingDisabled}
            isPaddingDisabled={isPaddingDisabled}
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
                            onClick={onRowClick ? () => onRowClick(row) : undefined}
                            style={{
                                transform: `translateY(${virtualRow.start}px)`,
                                height: `${virtualRow.size}px`,
                            }}
                            className={classNames({ "is-odd": virtualRow.index % 2 !== 0 }, getRowClassName?.(row))}
                        >
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
                        </tr>
                    );
                })}
            </tbody>
        </VirtualTableBodyWrapper>
    );
}

// Allows filtering by object content
const stringifyIncludesFilter: FilterFn<any> = (row, columnId, value: string): boolean => {
    const rowsToCheck = [row, ...row.getParentRows()];
    return rowsToCheck.some((r) => isRowIncludesValue(r, columnId, value));
};

function isRowIncludesValue<T>(row: Row<T>, columnId: string, value: string): boolean {
    const cellValue = row.getValue(columnId);
    const lowercaseFilter = value.toLowerCase();

    if (typeof cellValue === "object") {
        return JSON.stringify(values(cellValue)).toLowerCase().includes(lowercaseFilter);
    }

    return String(cellValue).toLowerCase().includes(lowercaseFilter);
}
