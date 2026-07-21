import { useLayoutEffect, type RefObject } from "react";
import type { ColumnSizingState, Table as ReactTable } from "@tanstack/react-table";

const CELL_PADDING_X = 16;
const RESIZER_WIDTH = 4;
const CELL_MIN_WIDTH = 30;
const CELL_MAX_WIDTH = 300;

function clamp(value: number, min: number, max: number): number {
    return Math.min(Math.max(value, min), max);
}

// Measures a cell's intrinsic content width, ignoring the column's current width so that
// truncated text still reports how wide it wants to be. The temporary style changes are
// reverted before the browser paints (this only runs inside useLayoutEffect).
function measureContentWidth(cell: HTMLElement): number {
    const content = cell.firstElementChild;
    if (!content) {
        return 0;
    }

    const { width, maxWidth } = cell.style;
    cell.style.width = "max-content";
    cell.style.maxWidth = "none";
    const measured = content.scrollWidth;
    cell.style.width = width;
    cell.style.maxWidth = maxWidth;

    return measured;
}

// Widest content across the column's header and (virtualized) body cells.
function measureColumnWidth(container: HTMLElement, columnId: string): number {
    const cells = container.querySelectorAll<HTMLElement>(`[data-column-id="${CSS.escape(columnId)}"]`);

    let widest = 0;
    cells.forEach((cell) => {
        widest = Math.max(widest, measureContentWidth(cell) + CELL_PADDING_X + RESIZER_WIDTH);
    });
    return widest;
}

function computeColumnSizing<TData>(
    table: ReactTable<TData>,
    container: HTMLElement,
    containerWidth: number,
    ratchetedWidths: ColumnSizingState,
): ColumnSizingState {
    const sizing: ColumnSizingState = {};
    const resizableIds: string[] = [];
    let totalWidth = 0;

    for (const column of table.getVisibleLeafColumns()) {
        // Columns that opt out of resizing (e.g. the checkbox column) keep their configured width.
        if (!column.getCanResize()) {
            totalWidth += column.getSize();
            continue;
        }

        const measured = clamp(measureColumnWidth(container, column.id), CELL_MIN_WIDTH, CELL_MAX_WIDTH);
        // Only the virtualized rows currently in the DOM can be measured, so a re-measure after a
        // container resize samples a different row window. If widths could shrink when a wide row
        // leaves that window, the layout can oscillate forever (narrower column -> scrollbar
        // disappears -> container grows -> the wide row is back -> wider column -> ...), so within
        // one measuring session widths only ever ratchet up.
        const width = Math.max(measured, ratchetedWidths[column.id] ?? 0);
        ratchetedWidths[column.id] = width;
        sizing[column.id] = width;
        totalWidth += width;
        resizableIds.push(column.id);
    }

    // Grow columns to fill any leftover space so the table always spans its container.
    // Fill 1px short: clientWidth is rounded to whole pixels and can exceed the real fractional
    // content width, and overshooting it even by a sub-pixel toggles a phantom horizontal
    // scrollbar (which then cascades into a vertical one). The table's min-w-full covers the gap.
    const freeSpace = containerWidth - totalWidth - 1;
    if (freeSpace > 0 && resizableIds.length > 0) {
        const cappedIds = resizableIds.filter((id) => sizing[id] === CELL_MAX_WIDTH);
        if (cappedIds.length > 0) {
            cappedIds.forEach((id) => {
                sizing[id] += Math.floor(freeSpace / cappedIds.length);
            });
        } else {
            const share = Math.floor(freeSpace / resizableIds.length);
            for (const id of resizableIds) {
                sizing[id] += share;
            }
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
 * resizes or web fonts finish loading. Columns that opt out of resizing keep their configured width.
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

        // Height-only container resizes (e.g. a scrollbar toggling, or a flex parent shrinking the
        // table) fire the observer too; skipping unchanged results keeps those from re-rendering
        // the table with an identical sizing state.
        const ratchetedWidths: ColumnSizingState = {};
        const resize = () => {
            const sizing = computeColumnSizing(table, container, container.clientWidth, ratchetedWidths);
            if (!isSameSizing(sizing, table.getState().columnSizing)) {
                table.setColumnSizing(sizing);
            }
        };

        resize();

        // Web fonts (the mono cells) can finish loading after the first pass, which would leave
        // columns sized to the narrower fallback metrics. Re-measure once fonts are ready.
        let cancelled = false;
        document.fonts?.ready.then(() => {
            if (!cancelled) {
                resize();
            }
        });

        const observer = new ResizeObserver(resize);
        observer.observe(container, { box: "content-box" });

        return () => {
            cancelled = true;
            observer.disconnect();
        };
    }, [table, containerRef, rowCount]);
}
