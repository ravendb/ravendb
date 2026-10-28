import { ColumnDef } from "@tanstack/react-table";
import { virtualTableConstants } from "components/common/virtualTable/utils/virtualTableConstants";
import { sumBy } from "common/typeUtils";

const {
    defaultRowHeightInPx,
    headerHeightInPx,
    paddingInPx,
    scrollbarWidthInPx,
    scrollbarHeightInPx,
    minTableHeightInPx,
} = virtualTableConstants;

interface TableBodyWidthOptions {
    isWithoutTablePadding?: boolean;
    isWithoutTableScrollbar?: boolean;
    isWithRightSpace?: boolean;
}

function getTableBodyWidth(containerWidth = 0, options: TableBodyWidthOptions = {}) {
    const { isWithoutTablePadding = true, isWithoutTableScrollbar = true, isWithRightSpace = true } = options;

    if (containerWidth === 0) {
        containerWidth = $(".react-container")[0]?.clientWidth ?? window.innerWidth;
    }

    if (isWithoutTablePadding) {
        // left and right padding
        containerWidth -= paddingInPx;
    }
    if (isWithoutTableScrollbar) {
        containerWidth -= scrollbarWidthInPx;
    }
    if (isWithRightSpace) {
        containerWidth -= 20; // some space to avoid horizontal scroll
    }

    return containerWidth;
}

function getCellSizeProvider(tableWidthInPx = 0) {
    if (tableWidthInPx === 0) {
        tableWidthInPx = getTableBodyWidth();
    }

    return (percentage: number) => Math.floor((tableWidthInPx * percentage) / 100);
}

function fitColumnsToWidth<T>(columns: ColumnDef<T>[], containerWidth: number): ColumnDef<T>[] {
    const isRelative = (column: ColumnDef<T>) => column.meta?.relativeWidth > 0;

    const fixedWidth = sumBy(
        columns.filter((column) => !isRelative(column)),
        (column) => column.size ?? 0
    );
    const totalRelativeWidth = sumBy(columns.filter(isRelative), (column) => column.meta.relativeWidth);
    const freeWidth = getTableBodyWidth(containerWidth) - fixedWidth;

    return columns.map((column) =>
        isRelative(column)
            ? { ...column, size: Math.floor((freeWidth * column.meta.relativeWidth) / totalRelativeWidth) }
            : column
    );
}

function getHeightInPx(rowsCount: number, maxHeightInPx: number, rowHeight = defaultRowHeightInPx): number {
    const calculatedHeightInPx =
        Math.max(rowHeight, rowsCount * rowHeight) + paddingInPx + headerHeightInPx + scrollbarHeightInPx;

    return Math.min(calculatedHeightInPx, maxHeightInPx);
}

function getTableContainerHeightInPx(heightInPx: number, isPaddingDisabled = false) {
    return Math.max(heightInPx - (isPaddingDisabled ? 0 : paddingInPx), minTableHeightInPx);
}

export const virtualTableUtils = {
    getCellSizeProvider,
    fitColumnsToWidth,
    getTableBodyWidth,
    getHeightInPx,
    getTableContainerHeightInPx,
};
