import { Row } from "@tanstack/react-table";
import classNames from "classnames";
import { memo } from "react";
import VirtualTableCells from "./VirtualTableCells";

interface LazyVirtualTableRowProps<T> {
    row: Row<T>;
    rowIndex: number;
    firstRowIndex: number;
    rowHeightInPx: number;
    isCompact?: boolean;
    isSelected: boolean;
    isSelectionPreview: boolean;
    renderDependencies: unknown[];
}

function LazyVirtualTableRow<T>({
    row,
    rowIndex,
    firstRowIndex,
    rowHeightInPx,
    isCompact,
    isSelected,
    isSelectionPreview,
}: LazyVirtualTableRowProps<T>) {
    return (
        <tr
            data-row-index={rowIndex}
            style={{
                height: rowHeightInPx,
                transform: `translateY(${(rowIndex - firstRowIndex) * rowHeightInPx}px)`,
            }}
            className={classNames({
                "is-odd": rowIndex % 2 !== 0,
                "is-selected": isSelected,
                "selection-preview": isSelectionPreview,
            })}
        >
            <VirtualTableCells row={row} isCompact={isCompact} />
        </tr>
    );
}

function areRowPropsEqual<T>(prev: LazyVirtualTableRowProps<T>, next: LazyVirtualTableRowProps<T>) {
    return (
        prev.row.original === next.row.original &&
        prev.rowIndex === next.rowIndex &&
        prev.firstRowIndex === next.firstRowIndex &&
        prev.rowHeightInPx === next.rowHeightInPx &&
        prev.isCompact === next.isCompact &&
        prev.isSelected === next.isSelected &&
        prev.isSelectionPreview === next.isSelectionPreview &&
        prev.renderDependencies.length === next.renderDependencies.length &&
        prev.renderDependencies.every((dependency, i) => dependency === next.renderDependencies[i])
    );
}

export default memo(LazyVirtualTableRow, areRowPropsEqual) as typeof LazyVirtualTableRow;
