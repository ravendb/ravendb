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

        const width = clamp(measureColumnWidth(container, column.id), CELL_MIN_WIDTH, CELL_MAX_WIDTH);
        sizing[column.id] = width;
        totalWidth += width;
        resizableIds.push(column.id);
    }

    // Grow columns to fill any leftover space so the table always spans its container.
    const freeSpace = containerWidth - totalWidth;
    if (freeSpace > 0 && resizableIds.length > 0) {
        const cappedId = resizableIds.find((id) => sizing[id] === CELL_MAX_WIDTH);
        if (cappedId) {
            // A capped column most likely holds the long content, so let it absorb the slack.
            sizing[cappedId] += freeSpace;
        } else {
            const share = Math.floor(freeSpace / resizableIds.length);
            for (const id of resizableIds) {
                sizing[id] += share;
            }
        }
    }

    return sizing;
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

        const resize = () => table.setColumnSizing(computeColumnSizing(table, container, container.clientWidth));

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
        observer.observe(container);

        return () => {
            cancelled = true;
            observer.disconnect();
        };
    }, [table, containerRef, rowCount]);
}
