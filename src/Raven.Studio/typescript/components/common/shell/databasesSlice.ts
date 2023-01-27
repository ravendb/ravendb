import { createEntityAdapter, createSlice, EntityState, PayloadAction } from "@reduxjs/toolkit";
import { DatabaseSharedInfo } from "components/models/databases";
import genUtils from "common/generalUtils";
import { RootState } from "components/store";

interface DatabasesState {
    databases: EntityState<DatabaseSharedInfo>;
    activeDatabase: string;
}

const databasesAdapter = createEntityAdapter<DatabaseSharedInfo>({
    selectId: (x) => x.name,
    sortComparer: (a, b) => genUtils.sortAlphaNumeric(a.name, b.name),
});

const databasesSelectors = databasesAdapter.getSelectors();

const initialState: DatabasesState = {
    databases: databasesAdapter.getInitialState(),
    activeDatabase: null,
};

const sliceName = "databases";

export const selectAllDatabases = (store: RootState) => databasesSelectors.selectAll(store.databases.databases);

export const selectActiveDatabase = (store: RootState) => store.databases.activeDatabase;

export function selectDatabaseByName(name: string) {
    return (store: RootState) => databasesSelectors.selectById(store.databases.databases, name);
}

export const databasesSlice = createSlice({
    initialState,
    name: sliceName,
    reducers: {
        activeDatabaseChanged: (state, action: PayloadAction<string>) => {
            state.activeDatabase = action.payload;
        },
        databasesLoaded: (state, action: PayloadAction<DatabaseSharedInfo[]>) => {
            //TODO: update in shallow mode?
            databasesAdapter.setAll(state.databases, action.payload);
        },
    },
});

export const { databasesLoaded, activeDatabaseChanged } = databasesSlice.actions;
