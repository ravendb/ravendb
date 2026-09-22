import { virtualTableConstants } from "components/common/virtualTable/utils/virtualTableConstants";

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

function getHeightInPx(rowsCount: number, maxHeightInPx: number, rowHeight = defaultRowHeightInPx): number {
    const calculatedHeightInPx =
        Math.max(rowHeight, rowsCount * rowHeight) + paddingInPx + headerHeightInPx + scrollbarHeightInPx;

    return Math.min(calculatedHeightInPx, maxHeightInPx);
}

// height of the scrollable container, the rest of heightInPx is the padding around the table
function getTableContainerHeightInPx(heightInPx: number, isPaddingDisabled = false) {
    return Math.max(heightInPx - (isPaddingDisabled ? 0 : paddingInPx), minTableHeightInPx);
}

export const virtualTableUtils = {
    getCellSizeProvider,
    getTableBodyWidth,
    getHeightInPx,
    getTableContainerHeightInPx,
};
