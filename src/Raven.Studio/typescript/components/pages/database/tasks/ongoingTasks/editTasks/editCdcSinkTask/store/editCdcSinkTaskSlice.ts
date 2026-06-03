import { createSlice, PayloadAction } from "@reduxjs/toolkit";
import { RootState } from "components/store";
import { EditCdcSinkTaskFormData } from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/utils/editCdcSinkTaskValidation";
import { FieldPath } from "react-hook-form/dist/types/path/eager";
import {
    EmbeddedTablePath,
    LinkedTablePath,
    RootTablePath,
} from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/utils/editCdcSinkTaskTypes";
import { CdcSinkSourceSchema } from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/utils/editCdcSinkTaskSchemaUtils";
import storageKeyProvider = require("common/storage/storageKeyProvider");

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
    sourceSchema: CdcSinkSourceSchema;
    activeTable?: CdcActiveTable;
    expandedTables: Partial<Record<FieldPath<EditCdcSinkTaskFormData>, boolean>>;
    isFieldMappingExpandedByDefault: boolean;
    isRawView: boolean;
    rawViewContent: string;
    taskId: number;
}

export const editCdcSinkTaskStorageKeys = {
    isFieldMappingExpandedByDefault: storageKeyProvider.storageKeyFor(
        "editCdcSinkTask.isFieldMappingExpandedByDefault"
    ),
};

function getStoredBoolean(key: string, defaultValue: boolean): boolean {
    const value = localStorage.getItem(key);
    return value == null ? defaultValue : value === "true";
}

const initialState: EditCdcSinkTaskState = {
    selectedConnectionString: null,
    sourceSchema: null,
    activeTable: null,
    expandedTables: {},
    isFieldMappingExpandedByDefault: getStoredBoolean(
        editCdcSinkTaskStorageKeys.isFieldMappingExpandedByDefault,
        false
    ),
    isRawView: false,
    rawViewContent: null,
    taskId: null,
};

export const editCdcSinkTaskSlice = createSlice({
    name: "editCdcSinkTask",
    initialState,
    reducers: {
        taskIdSet: (state, action: PayloadAction<number>) => {
            state.taskId = action.payload;
        },
        connectionStringSelected: (state, action: PayloadAction<SqlConnectionString>) => {
            state.selectedConnectionString = action.payload;
            state.sourceSchema = null;
        },
        sourceSchemaSet: (state, action: PayloadAction<CdcSinkSourceSchema>) => {
            state.sourceSchema = action.payload;
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
        fieldMappingExpandedByDefaultSet: (state, action: PayloadAction<boolean>) => {
            state.isFieldMappingExpandedByDefault = action.payload;
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
    taskId: (state: RootState) => state.editCdcSinkTask.taskId,
    selectedConnectionString: (state: RootState) => state.editCdcSinkTask.selectedConnectionString,
    sourceSchema: (state: RootState) => state.editCdcSinkTask.sourceSchema,
    activeTable: (state: RootState) => state.editCdcSinkTask.activeTable,
    isActiveTable: (path: CdcActiveTable["path"]) => (state: RootState) =>
        state.editCdcSinkTask.activeTable?.path === path,
    expandedTables: (state: RootState) => state.editCdcSinkTask.expandedTables,
    isFieldMappingExpandedByDefault: (state: RootState) => state.editCdcSinkTask.isFieldMappingExpandedByDefault,
    isRawView: (state: RootState) => state.editCdcSinkTask.isRawView,
    rawViewContent: (state: RootState) => state.editCdcSinkTask.rawViewContent,
};
