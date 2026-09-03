import { useLayoutEffect, type RefObject } from "react";
import type { ColumnSizingState, Table as ReactTable } from "@tanstack/react-table";

const CELL_PADDING_X = 16;
const RESIZER_WIDTH = 4;
const CELL_MIN_WIDTH = 30;
const CELL_MAX_WIDTH = 300;
const SCROLL_IDLE_MEASURE_DELAY_MS = 150;

function clamp(value: number, min: number, max: number): number {
    return Math.min(Math.max(value, min), max);
}

// Measures the intrinsic content width of each column. All cells are widened first and read
// afterwards (interleaving writes with scrollWidth reads forces a layout pass per cell); the
// temporary styles are reverted synchronously, so the browser never paints them.
// The result only ratchets up from previousWidths: only the virtualized rows currently in the
// DOM can be measured, and letting a column shrink when a wide row scrolls out of view would
// make the layout oscillate.
function measureColumnWidths(
    container: HTMLElement,
    columnIds: string[],
    previousWidths: ColumnSizingState,
): ColumnSizingState {
    const cellsByColumn = columnIds.map((columnId) => ({
        columnId,
        cells: [...container.querySelectorAll<HTMLElement>(`[data-column-id="${CSS.escape(columnId)}"]`)],
    }));

    const savedStyles = cellsByColumn.flatMap(({ cells }) =>
        cells.map((cell) => ({ cell, width: cell.style.width, maxWidth: cell.style.maxWidth })),
    );
    for (const { cell } of savedStyles) {
        cell.style.width = "max-content";
        cell.style.maxWidth = "none";
    }

    const widths: ColumnSizingState = {};
    for (const { columnId, cells } of cellsByColumn) {
        let widest = 0;
        for (const cell of cells) {
            const content = cell.firstElementChild;
            if (content) {
                widest = Math.max(widest, content.scrollWidth + CELL_PADDING_X + RESIZER_WIDTH);
            }
        }
        widths[columnId] = Math.max(clamp(widest, CELL_MIN_WIDTH, CELL_MAX_WIDTH), previousWidths[columnId] ?? 0);
    }

    for (const { cell, width, maxWidth } of savedStyles) {
        cell.style.width = width;
        cell.style.maxWidth = maxWidth;
    }

    return widths;
}

// Distributes the container width across columns from the measured content widths.
// Pure arithmetic (no DOM reads), so it is cheap enough to run on every resize event.
function distributeColumnSizing<TData>(
    table: ReactTable<TData>,
    containerWidth: number,
    contentWidths: ColumnSizingState,
): ColumnSizingState {
    const sizing: ColumnSizingState = {};
    const resizableIds: string[] = [];
    let totalWidth = 0;

    for (const column of table.getVisibleLeafColumns()) {
        if (!column.getCanResize()) {
            totalWidth += column.getSize();
            continue;
        }

        const width = contentWidths[column.id] ?? CELL_MIN_WIDTH;
        sizing[column.id] = width;
        totalWidth += width;
        resizableIds.push(column.id);
    }

    // Fill leftover space so the table spans its container, but stay 1px short: clientWidth is
    // rounded to whole pixels, and overshooting the real fractional width by even a sub-pixel
    // toggles a phantom horizontal scrollbar. The table's min-w-full covers the gap.
    const freeSpace = containerWidth - totalWidth - 1;
    if (freeSpace > 0 && resizableIds.length > 0) {
        const cappedIds = resizableIds.filter((id) => sizing[id] === CELL_MAX_WIDTH);
        const growIds = cappedIds.length > 0 ? cappedIds : resizableIds;
        const share = Math.floor(freeSpace / growIds.length);
        for (const id of growIds) {
            sizing[id] += share;
        }
    }

    return sizing;
}

function isSameSizing(next: ColumnSizingState, current: ColumnSizingState): boolean {
    const nextIds = Object.keys(next);
    return nextIds.length === Object.keys(current).length && nextIds.every((id) => next[id] === current[id]);
}

/**
 * Sizes each column to fit its content after layout, then keeps it in sync when the container
 * resizes, new virtualized rows scroll into view, or web fonts finish loading. Columns that opt
 * out of resizing keep their configured width.
 */
export function useAutoSizeColumns<TData>(
    table: ReactTable<TData>,
    containerRef: RefObject<HTMLDivElement | null>,
    rowCount: number,
) {
    useLayoutEffect(() => {
        const container = containerRef.current;
        if (!container || rowCount === 0) {
            return;
        }

        let contentWidths: ColumnSizingState = {};

        // Skipping identical results keeps height-only resizes (e.g. a scrollbar toggling)
        // from re-rendering the table with the same sizing state.
        const resize = () => {
            const sizing = distributeColumnSizing(table, container.clientWidth, contentWidths);
            if (!isSameSizing(sizing, table.getState().columnSizing)) {
                table.setColumnSizing(sizing);
            }
        };

        // Container resizes cannot change cell content, so the observer only redistributes
        // already measured widths.
        const measureAndResize = () => {
            const resizableIds = table
                .getVisibleLeafColumns()
                .filter((column) => column.getCanResize())
                .map((column) => column.id);
            contentWidths = measureColumnWidths(container, resizableIds, contentWidths);
            resize();
        };

        measureAndResize();

        // Web fonts can finish loading after the first pass, leaving columns sized to the
        // narrower fallback font metrics.
        let cancelled = false;
        document.fonts?.ready.then(() => {
            if (!cancelled) {
                measureAndResize();
            }
        });

        // Virtualized rows mount as the user scrolls, so wider content deeper in the list is
        // not in the DOM for the initial pass. Re-measure once scrolling pauses; ratcheting
        // makes the extra passes oscillation-free.
        let scrollIdleTimer: number | undefined;
        const handleScroll = () => {
            window.clearTimeout(scrollIdleTimer);
            scrollIdleTimer = window.setTimeout(measureAndResize, SCROLL_IDLE_MEASURE_DELAY_MS);
        };
        container.addEventListener("scroll", handleScroll, { passive: true });

        const observer = new ResizeObserver(resize);
        observer.observe(container, { box: "content-box" });

        return () => {
            cancelled = true;
            window.clearTimeout(scrollIdleTimer);
            container.removeEventListener("scroll", handleScroll);
            observer.disconnect();
        };
    }, [table, containerRef, rowCount]);
}
