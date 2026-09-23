import { virtualTableConstants } from "./virtualTableConstants";
import { virtualTableUtils } from "./virtualTableUtils";

// a half open range of absolute row indices
export interface RowRange {
    start: number;
    end: number;
}

const lazyVirtualTablePageSizeOptions = [25, 50, 100];

// autoResetPageIndex is off so tanstack does not reset its (unused) page index on every fetch
export const lazyTableOptions = {
    autoResetPageIndex: false,
    columnResizeMode: "onChange",
    defaultColumn: { enableSorting: false, enableColumnFilter: false },
} as const;

export function isInRange(range: RowRange, rowIndex: number) {
    return rowIndex >= range.start && rowIndex < range.end;
}

export function isSameRange(a: RowRange, b: RowRange) {
    return a.start === b.start && a.end === b.end;
}

export function findMissingRange(range: RowRange, isAvailable: (rowIndex: number) => boolean): RowRange | null {
    let first = -1;
    let last = -1;

    for (let i = range.start; i < range.end; i++) {
        if (!isAvailable(i)) {
            if (first === -1) {
                first = i;
            }
            last = i;
        }
    }

    return first === -1 ? null : { start: first, end: last + 1 };
}

export function snapDown(value: number, step: number) {
    return Math.floor(value / step) * step;
}

export function snapUp(value: number, step: number) {
    return Math.ceil(value / step) * step;
}

// number of rows that fit into the table when paginated, so the page does not need to scroll
export function getFitPageSize(heightInPx: number, rowHeightInPx: number, isCompact: boolean) {
    const headerHeightInPx = isCompact
        ? virtualTableConstants.compactHeaderHeightInPx
        : virtualTableConstants.headerHeightInPx;

    const rowsHeightInPx = virtualTableUtils.getTableContainerHeightInPx(heightInPx) - headerHeightInPx;

    return Math.max(1, Math.floor(rowsHeightInPx / rowHeightInPx));
}

// the default size followed by the larger predefined sizes, skipping the ones that would fit all rows into a single page
export function getPageSizeOptions(defaultPageSize: number, totalCount: number | null) {
    const largerSizes = lazyVirtualTablePageSizeOptions.filter(
        (x) => x > defaultPageSize && (totalCount === null || x < totalCount)
    );

    return [defaultPageSize, ...largerSizes];
}
