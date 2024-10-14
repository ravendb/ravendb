import { virtualTableConstants } from "components/common/virtualTable/utils/virtualTableConstants";

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
        containerWidth -= virtualTableConstants.paddingInPx;
    }
    if (isWithoutTableScrollbar) {
        containerWidth -= virtualTableConstants.scrollbarWidthInPx;
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

export const virtualTableUtils = {
    getCellSizeProvider,
    getTableBodyWidth,
};
