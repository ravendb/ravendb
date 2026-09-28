import { Row } from "@tanstack/react-table";
import classNames from "classnames";
import { memo } from "react";
import VirtualTableCells from "./VirtualTableCells";
import { virtualTableConstants } from "../utils/virtualTableConstants";

interface LazyVirtualTableRowProps<T> {
    row: Row<T>;
    rowIndex: number;
    positionInPage: number;
    isSelected: boolean;
    isSelectionPreview: boolean;
    renderDependencies: unknown[];
}

const { defaultRowHeightInPx } = virtualTableConstants;

function LazyVirtualTableRow<T>({
    row,
    rowIndex,
    positionInPage,
    isSelected,
    isSelectionPreview,
}: LazyVirtualTableRowProps<T>) {
    return (
        <tr
            data-row-index={rowIndex}
            style={{
                height: defaultRowHeightInPx,
                transform: `translateY(${positionInPage * defaultRowHeightInPx}px)`,
            }}
            className={classNames({
                "is-odd": rowIndex % 2 !== 0,
                "is-selected": isSelected,
                "selection-preview": isSelectionPreview,
            })}
        >
            <VirtualTableCells row={row} />
        </tr>
    );
}

function areRowPropsEqual<T>(prev: LazyVirtualTableRowProps<T>, next: LazyVirtualTableRowProps<T>) {
    return (
        prev.row.original === next.row.original &&
        prev.rowIndex === next.rowIndex &&
        prev.positionInPage === next.positionInPage &&
        prev.isSelected === next.isSelected &&
        prev.isSelectionPreview === next.isSelectionPreview &&
        prev.renderDependencies.every((dependency, i) => dependency === next.renderDependencies[i])
    );
}

export default memo(LazyVirtualTableRow, areRowPropsEqual) as typeof LazyVirtualTableRow;
