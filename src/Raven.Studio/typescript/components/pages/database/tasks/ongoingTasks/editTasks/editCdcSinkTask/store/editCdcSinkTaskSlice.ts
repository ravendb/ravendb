import { createSlice, PayloadAction } from "@reduxjs/toolkit";
import { RootState } from "components/store";
import { EditCdcSinkTaskFormData } from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/utils/editCdcSinkTaskValidation";
import { FieldPath } from "react-hook-form/dist/types/path/eager";
import {
    EmbeddedTablePath,
    LinkedTablePath,
    RootTablePath,
} from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/utils/editCdcSinkTaskFormPaths";

export type CdcTableType = "root" | "embedded" | "linked";

export type FormTableInfo =
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
    activeTable?: FormTableInfo;
    expandedTables: Partial<Record<FieldPath<EditCdcSinkTaskFormData>, boolean>>;
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
        activeTableSet: (state, action: PayloadAction<FormTableInfo>) => {
            state.activeTable = action.payload;
        },
        tableExpandedOneToggled: (state, action: PayloadAction<FieldPath<EditCdcSinkTaskFormData>>) => {
            const tableName = action.payload;
            state.expandedTables[tableName] = !state.expandedTables[tableName];
        },
        tableExpandedSet: (
            state,
            action: PayloadAction<Partial<Record<FieldPath<EditCdcSinkTaskFormData>, boolean>>>
        ) => {
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
