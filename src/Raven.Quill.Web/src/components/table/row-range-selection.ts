import { useEffect, useRef, useState } from "react";
import type { Row, RowSelectionState, Table } from "@tanstack/react-table";

/** CSS utility (defined in index.css) marking the rows a pending shift-click range would cover. */
export const RANGE_PREVIEW_ROW_CLASSNAME = "table-row-range-preview";

export function countSelectedRows(selection: RowSelectionState): number {
    return Object.values(selection).filter(Boolean).length;
}

function isRowSelected<TData>(table: Table<TData>, rowId: string | null): boolean {
    return rowId !== null && Boolean(table.getState().rowSelection[rowId]);
}

/**
 * The rows a shift-click on `targetRowId` would cover, and the state it would give them, already
 * trimmed to the selection limit.
 *
 * `isSelecting` follows the row the click lands on, so the range takes the same toggle that row's
 * own checkbox would: a shift-click on an unselected row extends the selection over the range, and
 * one on a selected row clears it. Reading it off the anchor instead would strand the gesture -
 * with nothing selected the anchor is unselected too, so the click would clear an already empty
 * range and do nothing at all.
 *
 * The range runs from the top of the table when there is no usable anchor. Only a plain click on a
 * row records one, so a selection that arrived some other way - seeded from a stored configuration,
 * taken with the header checkbox, cleared through "Deselect all" - has none, and a filter can hide
 * the row that held it. Without a fallback endpoint the shift-click in those states would collapse
 * into a plain toggle, which reads as Shift doing nothing at all.
 */
export function getRangeSelection<TData>(
    table: Table<TData>,
    anchorRowId: string | null,
    targetRowId: string,
    maxSelectedCount: number = Infinity,
): { rows: Row<TData>[]; isSelecting: boolean } {
    const isSelecting = !isRowSelected(table, targetRowId);
    const rows = table.getRowModel().rows;
    const targetIndex = rows.findIndex((row) => row.id === targetRowId);

    if (targetIndex === -1) {
        return { rows: [], isSelecting };
    }

    // findIndex reports -1 for both a missing and a null anchor, which the clamp turns into the
    // first row; the empty-table case is already ruled out by the target lookup above.
    const anchorIndex = Math.max(
        rows.findIndex((row) => row.id === anchorRowId),
        0,
    );
    const rangeRows = rows.slice(Math.min(anchorIndex, targetIndex), Math.max(anchorIndex, targetIndex) + 1);

    return { rows: takeWithinLimit(table, rangeRows, isSelecting, maxSelectedCount), isSelecting };
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
                getRangeSelection(table, anchorRowIdRef.current, hoveredRowId, maxSelectedCount).rows.map(
                    (row) => row.id,
                ),
            );
        },
    };
}
