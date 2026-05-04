import { createSlice, PayloadAction } from "@reduxjs/toolkit";
import { RootState } from "components/store";

export interface FormTableInfo {
    type: "root" | "linked" | "embedded";
    path: string;
    label: string;
}

interface ActiveTable {
    parents: FormTableInfo[];
    current: FormTableInfo;
}

interface EditCdcSinkTaskState {
    selectedConnectionString: SqlConnectionString;
    activeTable?: ActiveTable;
    expandedTables: Record<string, boolean>;
}

const initialState: EditCdcSinkTaskState = {
    selectedConnectionString: null,
    activeTable: null,
    expandedTables: {},
};

export const editCdcSinkTaskSlice = createSlice({
    name: "editCdcSinkTask",
    initialState,
    reducers: {
        connectionStringSelected: (state, action: PayloadAction<SqlConnectionString>) => {
            state.selectedConnectionString = action.payload;
        },
        activeTableSet: (state, action: PayloadAction<ActiveTable>) => {
            state.activeTable = action.payload;
        },
        tableExpandedOneToggled: (state, action: PayloadAction<string>) => {
            const tableName = action.payload;
            state.expandedTables[tableName] = !state.expandedTables[tableName];
        },
        tableExpandedSet: (state, action: PayloadAction<Record<string, boolean>>) => {
            state.expandedTables = action.payload;
        },
        reset: () => initialState,
    },
});

export const editCdcSinkTaskActions = editCdcSinkTaskSlice.actions;

export const editCdcSinkTaskSelectors = {
    selectedConnectionString: (state: RootState) => state.editCdcSinkTask.selectedConnectionString,
    activeTable: (state: RootState) => state.editCdcSinkTask.activeTable,
    expandedTables: (state: RootState) => state.editCdcSinkTask.expandedTables,
};
