import { RowSelectionState } from "@tanstack/react-table";
import { LazyRow } from "components/common/virtualTable/hooks/useLazyVirtualTable";
import { SelectionState } from "components/models/common";
import document from "models/database/documents/document";
import { useState } from "react";

export type DocumentsSelectionState =
    | { mode: "inclusive"; selectedIds: string[] }
    | { mode: "exclusive"; excludedIds: string[] };

export interface DocumentsSelection {
    state: DocumentsSelectionState;
    selectionState: SelectionState;
    selectedCount: number;
    rowSelection: RowSelectionState;
    anchorRowIndex: number | null;
    toggleRow: (dataIndex: number, isRangeSelection: boolean) => void;
    toggleAll: () => void;
    clear: () => void;
}

interface UseDocumentsSelectionProps {
    rows: LazyRow<document>[];
    totalCount: number | null;
    isPaginated: boolean;
}

const emptySelection: DocumentsSelectionState = { mode: "inclusive", selectedIds: [] };
const allSelection: DocumentsSelectionState = { mode: "exclusive", excludedIds: [] };

export function useDocumentsSelection({ rows, totalCount, isPaginated }: UseDocumentsSelectionProps) {
    const [state, setState] = useState<DocumentsSelectionState>(emptySelection);
    const [anchorRowIndex, setAnchorRowIndex] = useState<number>(null);

    const selectedIds = rows.map((x) => x.item.getId()).filter((id) => isDocumentSelected(state, id));

    const selectionState = isPaginated
        ? getPageSelectionState(selectedIds.length, rows.length)
        : getSelectionState(state);

    const toggleRow = (dataIndex: number, isRangeSelection: boolean) => {
        const { index: rowIndex, item } = rows[dataIndex];
        const isSelected = !isDocumentSelected(state, item.getId());

        const ids =
            isRangeSelection && anchorRowIndex !== null
                ? getIdsInRange(rows, anchorRowIndex, rowIndex)
                : [item.getId()];

        setState(withSelected(state, ids, isSelected));
        setAnchorRowIndex(rowIndex);
    };

    const toggleAll = () => {
        if (isPaginated) {
            const pageIds = rows.map((x) => x.item.getId());
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

    const selection: DocumentsSelection = {
        state,
        selectionState,
        selectedCount,
        rowSelection: Object.fromEntries(selectedIds.map((id) => [id, true])),
        anchorRowIndex,
        toggleRow,
        toggleAll,
        clear,
    };

    return selection;
}

function getIdsInRange(rows: LazyRow<document>[], fromRowIndex: number, toRowIndex: number) {
    const start = Math.min(fromRowIndex, toRowIndex);
    const end = Math.max(fromRowIndex, toRowIndex);

    return rows.filter((x) => x.index >= start && x.index <= end).map((x) => x.item.getId());
}

function isDocumentSelected(state: DocumentsSelectionState, id: string) {
    return state.mode === "inclusive" ? state.selectedIds.includes(id) : !state.excludedIds.includes(id);
}

function withSelected(state: DocumentsSelectionState, ids: string[], isSelected: boolean): DocumentsSelectionState {
    if (state.mode === "inclusive") {
        const otherIds = state.selectedIds.filter((x) => !ids.includes(x));
        return { mode: "inclusive", selectedIds: isSelected ? [...otherIds, ...ids] : otherIds };
    }

    const otherIds = state.excludedIds.filter((x) => !ids.includes(x));
    return { mode: "exclusive", excludedIds: isSelected ? otherIds : [...otherIds, ...ids] };
}

function getSelectionState(state: DocumentsSelectionState): SelectionState {
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
