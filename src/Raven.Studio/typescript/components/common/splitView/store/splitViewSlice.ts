import { createSlice, PayloadAction } from "@reduxjs/toolkit";
import { RootState } from "components/store";

interface SplitViewState {
    isSheetPinned: boolean;
    initialPanelWidthInPx: number;
    minPanelWidthInPx: number;
    maxPanelWidthInPx: number;
    viewWidthInPx: number;
    sheetWidthInPx: number;
}

const initialState: SplitViewState = {
    isSheetPinned: false,
    initialPanelWidthInPx: 0,
    minPanelWidthInPx: 0,
    maxPanelWidthInPx: 0,
    viewWidthInPx: 0,
    sheetWidthInPx: 0,
};

export const splitViewSlice = createSlice({
    name: "splitView",
    initialState,
    reducers: {
        initialPanelWidthInPxSet: (state, action: PayloadAction<number>) => {
            state.initialPanelWidthInPx = action.payload;
        },
        minPanelWidthInPxSet: (state, action: PayloadAction<number>) => {
            state.minPanelWidthInPx = action.payload;
        },
        maxPanelWidthInPxSet: (state, action: PayloadAction<number>) => {
            state.maxPanelWidthInPx = action.payload;
        },
        isSheetPinnedSet: (state, action: PayloadAction<boolean>) => {
            state.isSheetPinned = action.payload;
        },
        viewWidthInPxSet: (state, action: PayloadAction<number>) => {
            state.viewWidthInPx = action.payload;
        },
        sheetWidthInPxSet: (state, action: PayloadAction<number>) => {
            state.sheetWidthInPx = action.payload;
        },
    },
});

export const splitViewActions = splitViewSlice.actions;

export const splitViewSelectors = {
    initialPanelWidthInPx: (state: RootState) => state.splitView.initialPanelWidthInPx,
    minPanelWidthInPx: (state: RootState) => state.splitView.minPanelWidthInPx,
    maxPanelWidthInPx: (state: RootState) => state.splitView.maxPanelWidthInPx,
    isSheetPinned: (state: RootState) => state.splitView.isSheetPinned,
    viewWidthInPx: (state: RootState) => state.splitView.viewWidthInPx,
    sheetWidthInPx: (state: RootState) => state.splitView.sheetWidthInPx,
};
