import { RowData, RowSelectionState } from "@tanstack/react-table";
import genUtils from "common/generalUtils";
import { LazyRows } from "components/common/virtualTable/hooks/useLazyRows";
import { SelectionState } from "components/models/common";
import { range } from "lodash";
import { useState } from "react";

export type LazyTableSelectionState =
    | { mode: "inclusive"; selectedIds: string[] }
    | { mode: "exclusive"; excludedIds: string[] };

export interface LazyTableSelection<T> {
    state: LazyTableSelectionState;
    selectionState: SelectionState;
    selectedCount: number;
    rowSelection: RowSelectionState;
    anchorRowIndex: number | null;
    canSelectRangeTo: (rowIndex: number) => boolean;
    toggleRow: (item: T, isRangeSelection: boolean) => void;
    toggleAll: () => void;
    clear: () => void;
}

interface UseLazyTableSelectionProps<T> {
    lazyRows: Pick<LazyRows<T>, "rows" | "getItem">;
    getId: (item: T) => string;
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
    lazyRows: { rows, getItem },
    getId,
    totalCount,
    isPaginated,
}: UseLazyTableSelectionProps<T>): LazyTableSelection<T> {
    const [state, setState] = useState<LazyTableSelectionState>(emptySelection);
    const [anchorRowIndex, setAnchorRowIndex] = useState<number>(null);

    const isSelected = createIsSelected(state);
    const rowIds = rows.map((x) => getId(x.item));
    const selectedRowIds = rowIds.filter(isSelected);

    const getItemsInRange = (rowIndex: number) => {
        const items = range(Math.min(anchorRowIndex, rowIndex), Math.max(anchorRowIndex, rowIndex) + 1).map(getItem);
        return items.includes(undefined) ? null : items;
    };

    const canSelectRangeTo = (rowIndex: number) => anchorRowIndex !== null && getItemsInRange(rowIndex) !== null;

    const toggleRow = (item: T, isRangeSelection: boolean) => {
        const rowIndex = rows.find((x) => x.item === item).index;
        const id = getId(item);
        const rangeItems = isRangeSelection && anchorRowIndex !== null ? getItemsInRange(rowIndex) : null;
        const ids = rangeItems ? rangeItems.map(getId) : [id];

        setState(withSelected(state, ids, !isSelected(id)));
        setAnchorRowIndex(rowIndex);
    };

    const selectionState = isPaginated ? genUtils.getSelectionState(rowIds, selectedRowIds) : getSelectionState(state);

    const toggleAll = () => {
        if (isPaginated) {
            setState(withSelected(state, rowIds, selectionState !== "AllSelected"));
        } else {
            setState(selectionState === "Empty" ? allSelection : emptySelection);
        }

        setAnchorRowIndex(null);
    };

    const clear = () => {
        setState(emptySelection);
        setAnchorRowIndex(null);
    };

    return {
        state,
        selectionState,
        selectedCount:
            state.mode === "inclusive"
                ? state.selectedIds.length
                : Math.max(0, (totalCount ?? 0) - state.excludedIds.length),
        rowSelection: Object.fromEntries(selectedRowIds.map((id) => [id, true])),
        anchorRowIndex,
        canSelectRangeTo,
        toggleRow,
        toggleAll,
        clear,
    };
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
