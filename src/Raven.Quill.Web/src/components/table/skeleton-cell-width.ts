// Content in a real column is a consistent width, so varying the placeholder by column rather
// than by cell is what makes it read as a table instead of a block of stripes.
const COLUMN_WIDTHS = ["w-32", "w-16", "w-24", "w-20", "w-28", "w-14"];
const ACTION_COLUMN_WIDTH = "w-8";

export function getSkeletonCellWidth(columnIndex: number, columnCount: number, hasActionColumn: boolean) {
    return hasActionColumn && columnIndex === columnCount - 1
        ? ACTION_COLUMN_WIDTH
        : COLUMN_WIDTHS[columnIndex % COLUMN_WIDTHS.length];
}
