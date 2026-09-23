import { RowData, RowSelectionState } from "@tanstack/react-table";
import { LazyRows } from "components/common/virtualTable/hooks/useLazyRows";
import { SelectionState } from "components/models/common";
import { useState } from "react";

export type LazyTableSelectionState =
    | { mode: "inclusive"; selectedIds: string[] }
    | { mode: "exclusive"; excludedIds: string[] };

export interface LazyTableSelection<T> {
    state: LazyTableSelectionState;
    selectionState: SelectionState;
    selectedCount: number;
    rowSelection: RowSelectionState;
    // absolute index of the row toggled last, a shift-click selects the range between it and the clicked row
    anchorRowIndex: number | null;
    toggleRow: (item: T, isRangeSelection: boolean) => void;
    toggleAll: () => void;
    clear: () => void;
}

interface UseLazyTableSelectionProps<T> {
    lazyRows: Pick<LazyRows<T>, "rows" | "getItem">;
    getId: (item: T) => string;
    // selecting all in scroll mode counts every row, not only the loaded ones
    totalCount: number | null;
    isPaginated: boolean;
}

declare module "@tanstack/react-table" {
    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    interface TableMeta<TData extends RowData> {
        lazySelection?: LazyTableSelection<TData>;
    }
}

const emptySelection: LazyTableSelectionState = { mode: "inclusive", selectedIds: [] };
const allSelection: LazyTableSelectionState = { mode: "exclusive", excludedIds: [] };

export function useLazyTableSelection<T>({
    lazyRows,
    getId,
    totalCount,
    isPaginated,
}: UseLazyTableSelectionProps<T>): LazyTableSelection<T> {
    const [state, setState] = useState<LazyTableSelectionState>(emptySelection);
    const [anchorRowIndex, setAnchorRowIndex] = useState<number>(null);

    const { rows, getItem } = lazyRows;

    const isSelected = createIsSelected(state);
    const selectedIds = rows.map((x) => getId(x.item)).filter(isSelected);

    const selectionState = isPaginated
        ? getPageSelectionState(selectedIds.length, rows.length)
        : getSelectionState(state);

    const toggleRow = (item: T, isRangeSelection: boolean) => {
        const rowIndex = rows.find((x) => x.item === item)?.index;
        if (rowIndex == null) {
            return;
        }

        const id = getId(item);
        const ids =
            isRangeSelection && anchorRowIndex !== null
                ? getLoadedIdsInRange(anchorRowIndex, rowIndex, getItem, getId)
                : [id];

        setState(withSelected(state, ids, !isSelected(id)));
        setAnchorRowIndex(rowIndex);
    };

    const toggleAll = () => {
        if (isPaginated) {
            const pageIds = rows.map((x) => getId(x.item));
            setState(withSelected(state, pageIds, selectionState !== "AllSelected"));
        } else {
            setState(selectionState === "Empty" ? allSelection : emptySelection);
        }

        setAnchorRowIndex(null);
    };

    const clear = () => {
        setState(emptySelection);
        setAnchorRowIndex(null);
    };

    const selectedCount =
        state.mode === "inclusive"
            ? state.selectedIds.length
            : Math.max(0, (totalCount ?? 0) - state.excludedIds.length);

    return {
        state,
        selectionState,
        selectedCount,
        rowSelection: Object.fromEntries(selectedIds.map((id) => [id, true])),
        anchorRowIndex,
        toggleRow,
        toggleAll,
        clear,
    };
}

// rows that were never loaded are skipped
function getLoadedIdsInRange<T>(
    fromRowIndex: number,
    toRowIndex: number,
    getItem: (rowIndex: number) => T | undefined,
    getId: (item: T) => string
) {
    const ids: string[] = [];

    for (let i = Math.min(fromRowIndex, toRowIndex); i <= Math.max(fromRowIndex, toRowIndex); i++) {
        const item = getItem(i);
        if (item !== undefined) {
            ids.push(getId(item));
        }
    }

    return ids;
}

function createIsSelected(state: LazyTableSelectionState) {
    if (state.mode === "inclusive") {
        const selectedIds = new Set(state.selectedIds);
        return (id: string) => selectedIds.has(id);
    }

    const excludedIds = new Set(state.excludedIds);
    return (id: string) => !excludedIds.has(id);
}

function withSelected(state: LazyTableSelectionState, ids: string[], isSelected: boolean): LazyTableSelectionState {
    const toggledIds = new Set(ids);

    if (state.mode === "inclusive") {
        const otherIds = state.selectedIds.filter((x) => !toggledIds.has(x));
        return { mode: "inclusive", selectedIds: isSelected ? [...otherIds, ...ids] : otherIds };
    }

    const otherIds = state.excludedIds.filter((x) => !toggledIds.has(x));
    return { mode: "exclusive", excludedIds: isSelected ? otherIds : [...otherIds, ...ids] };
}

function getSelectionState(state: LazyTableSelectionState): SelectionState {
    if (state.mode === "exclusive") {
        return state.excludedIds.length === 0 ? "AllSelected" : "SomeSelected";
    }

    return state.selectedIds.length > 0 ? "SomeSelected" : "Empty";
}

function getPageSelectionState(selectedCount: number, pageSize: number): SelectionState {
    if (selectedCount === 0) {
        return "Empty";
    }

    return selectedCount === pageSize ? "AllSelected" : "SomeSelected";
}
