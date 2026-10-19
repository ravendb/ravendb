export interface RowRange {
    start: number;
    end: number;
}

export const lazyTableOptions = {
    autoResetPageIndex: false,
    columnResizeMode: "onChange",
    defaultColumn: { enableSorting: false, enableColumnFilter: false },
} as const;

export function isSameRange(a: RowRange, b: RowRange) {
    return a.start === b.start && a.end === b.end;
}

export function findMissingRange(range: RowRange, isAvailable: (rowIndex: number) => boolean): RowRange | null {
    let start = range.start;
    while (start < range.end && isAvailable(start)) {
        start++;
    }

    if (start === range.end) {
        return null;
    }

    let end = range.end;
    while (isAvailable(end - 1)) {
        end--;
    }

    return { start, end };
}
