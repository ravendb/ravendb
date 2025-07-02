import { createSlice, PayloadAction } from "@reduxjs/toolkit";
import { RootState } from "components/store";

interface EditAiAgentState {
    isTestOpen: boolean;
}

const initialState: EditAiAgentState = {
    isTestOpen: false,
};

export const editAiAgentSlice = createSlice({
    name: "editAiAgent",
    initialState,
    reducers: {
        isTestOpenSet: (state, action: PayloadAction<boolean>) => {
            state.isTestOpen = action.payload;
        },
    },
});

export const editAiAgentActions = editAiAgentSlice.actions;

export const editAiAgentSelectors = {
    isTestOpen: (state: RootState) => state.editAiAgent.isTestOpen,
};
