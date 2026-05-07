import { createSlice, PayloadAction } from "@reduxjs/toolkit";
import { RootState } from "components/store";
import { EditCdcSinkTaskFormData } from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/utils/editCdcSinkTaskValidation";
import { FieldPath } from "react-hook-form/dist/types/path/eager";
import {
    EmbeddedTablePath,
    LinkedTablePath,
    RootTablePath,
} from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/utils/editCdcSinkTaskFormPaths";

export type CdcActiveTable =
    | {
          type: "root";
          path: RootTablePath;
      }
    | {
          type: "linked";
          path: LinkedTablePath;
      }
    | {
          type: "embedded";
          path: EmbeddedTablePath;
      };

interface EditCdcSinkTaskState {
    selectedConnectionString: SqlConnectionString;
    activeTable?: CdcActiveTable;
    expandedTables: Partial<Record<FieldPath<EditCdcSinkTaskFormData>, boolean>>;
    isRawView: boolean;
    rawViewContent: string;
}

const initialState: EditCdcSinkTaskState = {
    selectedConnectionString: null,
    activeTable: null,
    expandedTables: {},
    isRawView: false,
    rawViewContent: null,
};

export const editCdcSinkTaskSlice = createSlice({
    name: "editCdcSinkTask",
    initialState,
    reducers: {
        connectionStringSelected: (state, action: PayloadAction<SqlConnectionString>) => {
            state.selectedConnectionString = action.payload;
        },
        activeTableSet: (state, action: PayloadAction<CdcActiveTable>) => {
            state.activeTable = action.payload;
        },
        activeTableCleared: (state) => {
            state.activeTable = null;
        },
        tableExpandedOneToggled: (state, action: PayloadAction<FieldPath<EditCdcSinkTaskFormData>>) => {
            const tableName = action.payload;
            state.expandedTables[tableName] = !state.expandedTables[tableName];
        },
        tableExpandedOneSet: (
            state,
            action: PayloadAction<{ path: FieldPath<EditCdcSinkTaskFormData>; isExpanded: boolean }>
        ) => {
            state.expandedTables[action.payload.path] = action.payload.isExpanded;
        },
        tableExpandedRemoved: (state, action: PayloadAction<FieldPath<EditCdcSinkTaskFormData>>) => {
            const path = action.payload;
            Object.keys(state.expandedTables).forEach((expandedPath) => {
                if (expandedPath === path || expandedPath.startsWith(`${path}.`)) {
                    delete state.expandedTables[expandedPath as FieldPath<EditCdcSinkTaskFormData>];
                }
            });
        },
        tableExpandedSet: (
            state,
            action: PayloadAction<Partial<Record<FieldPath<EditCdcSinkTaskFormData>, boolean>>>
        ) => {
            state.expandedTables = action.payload;
        },
        rawViewToggled: (state) => {
            state.isRawView = !state.isRawView;
        },
        rawViewContentSet: (state, action: PayloadAction<string>) => {
            state.rawViewContent = action.payload;
        },
        reset: () => initialState,
    },
});

export const editCdcSinkTaskActions = editCdcSinkTaskSlice.actions;

export const editCdcSinkTaskSelectors = {
    selectedConnectionString: (state: RootState) => state.editCdcSinkTask.selectedConnectionString,
    activeTable: (state: RootState) => state.editCdcSinkTask.activeTable,
    expandedTables: (state: RootState) => state.editCdcSinkTask.expandedTables,
    isRawView: (state: RootState) => state.editCdcSinkTask.isRawView,
    rawViewContent: (state: RootState) => state.editCdcSinkTask.rawViewContent,
};
