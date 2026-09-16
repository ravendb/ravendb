import type { Row, RowSelectionState, Table } from "@tanstack/react-table";
import { describe, expect, it } from "vitest";
import { getRangeSelection, setRowsSelected } from "@/components/table/row-range-selection";

/**
 * The range helpers only read the row model and the selection state, and write the selection back,
 * so a stub covering those three is enough to pin the range rules without standing up react-table.
 */
function createTable(rowIds: string[], selectedRowIds: string[] = []) {
    let rowSelection: RowSelectionState = Object.fromEntries(selectedRowIds.map((id) => [id, true]));
    const rows = rowIds.map((id) => ({ id }) as Row<unknown>);

    const table = {
        getRowModel: () => ({ rows }),
        getState: () => ({ rowSelection }),
        setRowSelection: (next: RowSelectionState) => {
            rowSelection = next;
        },
    } as unknown as Table<unknown>;

    return {
        table,
        selectedIds: () => Object.keys(rowSelection).filter((id) => rowSelection[id]),
    };
}

const ROWS = ["a", "b", "c", "d", "e"];
const rangeIds = <TData>(range: { rows: Row<TData>[] }) => range.rows.map((row) => row.id);

describe("getRangeSelection", () => {
    it("selects the range when the clicked row is unselected, even with nothing selected yet", () => {
        const { table } = createTable(ROWS);

        const range = getRangeSelection(table, "a", "c");

        expect(rangeIds(range)).toEqual(["a", "b", "c"]);
        expect(range.isSelecting).toBe(true);
    });

    it("clears the range when the clicked row is already selected", () => {
        const { table } = createTable(ROWS, ["a", "b", "c"]);

        const range = getRangeSelection(table, "a", "c");

        expect(rangeIds(range)).toEqual(["a", "b", "c"]);
        expect(range.isSelecting).toBe(false);
    });

    it("covers the range in display order whichever side the anchor is on", () => {
        const { table } = createTable(ROWS);

        expect(rangeIds(getRangeSelection(table, "d", "b"))).toEqual(["b", "c", "d"]);
    });

    it("runs from the top row when no anchor has been recorded yet", () => {
        const { table } = createTable(ROWS);

        expect(rangeIds(getRangeSelection(table, null, "c"))).toEqual(["a", "b", "c"]);
    });

    it("runs from the top row once a filter has hidden the anchor", () => {
        const { table } = createTable(ROWS);

        expect(rangeIds(getRangeSelection(table, "missing", "c"))).toEqual(["a", "b", "c"]);
    });

    it("takes no range for a target that is not in the row model", () => {
        const { table } = createTable(ROWS);

        expect(rangeIds(getRangeSelection(table, "a", "missing"))).toEqual([]);
    });

    it("takes no range in an empty table", () => {
        const { table } = createTable([]);

        expect(rangeIds(getRangeSelection(table, null, "a"))).toEqual([]);
    });

    it("trims a selecting range to the limit", () => {
        const { table } = createTable(ROWS, ["a"]);

        const range = getRangeSelection(table, "b", "e", 3);

        // "a" already holds one of the three slots, leaving room for "b" and "c" only.
        expect(rangeIds(range)).toEqual(["b", "c"]);
    });

    it("never trims a clearing range, so a full range can always be cleared", () => {
        const { table } = createTable(ROWS, ROWS);

        const range = getRangeSelection(table, "a", "e", 3);

        expect(rangeIds(range)).toEqual(ROWS);
        expect(range.isSelecting).toBe(false);
    });
});

describe("setRowsSelected", () => {
    it("adds the range to the existing selection", () => {
        const { table, selectedIds } = createTable(ROWS, ["e"]);
        const range = getRangeSelection(table, "a", "b");

        setRowsSelected(table, range.rows, range.isSelecting);

        expect(selectedIds().sort()).toEqual(["a", "b", "e"]);
    });

    it("removes the range and leaves the rest of the selection alone", () => {
        const { table, selectedIds } = createTable(ROWS, ["a", "b", "e"]);
        const range = getRangeSelection(table, "a", "b");

        setRowsSelected(table, range.rows, range.isSelecting);

        expect(selectedIds()).toEqual(["e"]);
    });

    it("stops selecting at the limit", () => {
        const { table, selectedIds } = createTable(ROWS);

        setRowsSelected(
            table,
            [...Array(5).keys()].map((i) => ({ id: ROWS[i]! }) as Row<unknown>),
            true,
            2,
        );

        expect(selectedIds()).toEqual(["a", "b"]);
    });
});
