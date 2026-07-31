import { useEffect, useRef, useState } from "react";
import type { Row, RowSelectionState, Table } from "@tanstack/react-table";

/** CSS utility (defined in index.css) marking the rows a pending shift-click range would cover. */
export const RANGE_PREVIEW_ROW_CLASSNAME = "table-row-range-preview";

export function countSelectedRows(selection: RowSelectionState): number {
    return Object.values(selection).filter(Boolean).length;
}

export function isRowSelected<TData>(table: Table<TData>, rowId: string | null): boolean {
    return rowId !== null && Boolean(table.getState().rowSelection[rowId]);
}

/**
 * The rows a shift-click on `targetRowId` would apply the anchor row's state to, already trimmed to
 * the selection limit. Empty when there is no range to take, e.g. before the first click or once a
 * filter has removed the anchor; the click then falls back to toggling the row it landed on.
 */
export function getRangeSelectionRows<TData>(
    table: Table<TData>,
    anchorRowId: string | null,
    targetRowId: string,
    maxSelectedCount: number = Infinity,
): Row<TData>[] {
    if (anchorRowId === null) {
        return [];
    }

    const rows = table.getRowModel().rows;
    const anchorIndex = rows.findIndex((row) => row.id === anchorRowId);
    const targetIndex = rows.findIndex((row) => row.id === targetRowId);

    if (anchorIndex === -1 || targetIndex === -1) {
        return [];
    }

    const rangeRows = rows.slice(Math.min(anchorIndex, targetIndex), Math.max(anchorIndex, targetIndex) + 1);

    return takeWithinLimit(table, rangeRows, isRowSelected(table, anchorRowId), maxSelectedCount);
}

/** Selects or clears `rows`, in display order, without letting the selection pass the limit. */
export function setRowsSelected<TData>(
    table: Table<TData>,
    rows: Row<TData>[],
    isSelected: boolean,
    maxSelectedCount: number = Infinity,
): void {
    const selection: RowSelectionState = { ...table.getState().rowSelection };

    for (const row of takeWithinLimit(table, rows, isSelected, maxSelectedCount)) {
        if (isSelected) {
            selection[row.id] = true;
        } else {
            delete selection[row.id];
        }
    }

    table.setRowSelection(selection);
}

function takeWithinLimit<TData>(
    table: Table<TData>,
    rows: Row<TData>[],
    isSelected: boolean,
    maxSelectedCount: number,
): Row<TData>[] {
    if (!isSelected) {
        return rows;
    }

    const selection = table.getState().rowSelection;
    let selectedCount = countSelectedRows(selection);
    const allowedRows: Row<TData>[] = [];

    for (const row of rows) {
        if (!selection[row.id]) {
            if (selectedCount >= maxSelectedCount) {
                break;
            }

            selectedCount++;
        }

        allowedRows.push(row);
    }

    return allowedRows;
}

function useIsShiftHeld(): boolean {
    const [isShiftHeld, setIsShiftHeld] = useState(false);

    useEffect(() => {
        const sync = (event: KeyboardEvent) => setIsShiftHeld(event.shiftKey);
        // Leaving the window swallows the keyup, which would leave the preview stuck on.
        const clear = () => setIsShiftHeld(false);

        window.addEventListener("keydown", sync);
        window.addEventListener("keyup", sync);
        window.addEventListener("blur", clear);

        return () => {
            window.removeEventListener("keydown", sync);
            window.removeEventListener("keyup", sync);
            window.removeEventListener("blur", clear);
        };
    }, []);

    return isShiftHeld;
}

/** Anchor, hover, and Shift state a table needs to take and preview shift-click ranges. */
export function useRowRangeSelection<TData>(maxSelectedCount: number = Infinity) {
    // The row the last plain click landed on, from which a shift-click takes its range.
    const anchorRowIdRef = useRef<string | null>(null);
    const [hoveredRowId, setHoveredRowId] = useState<string | null>(null);
    const isShiftHeld = useIsShiftHeld();

    return {
        anchorRowIdRef,
        onRowHoverChange: setHoveredRowId,
        getPreviewRowIds(table: Table<TData>): Set<string> {
            if (!isShiftHeld || hoveredRowId === null) {
                return new Set();
            }

            return new Set(
                getRangeSelectionRows(table, anchorRowIdRef.current, hoveredRowId, maxSelectedCount).map(
                    (row) => row.id,
                ),
            );
        },
    };
}
