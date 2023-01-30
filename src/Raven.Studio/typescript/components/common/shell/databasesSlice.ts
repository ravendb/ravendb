import { createEntityAdapter, createSlice, EntityState, PayloadAction } from "@reduxjs/toolkit";
import { DatabaseSharedInfo } from "components/models/databases";
import genUtils from "common/generalUtils";
import { AppAsyncThunk, AppDispatch, RootState } from "components/store";
import createDatabase from "viewmodels/resources/createDatabase";
import app from "durandal/app";
import deleteDatabaseConfirm from "viewmodels/resources/deleteDatabaseConfirm";
import deleteDatabaseCommand from "commands/resources/deleteDatabaseCommand";
import DatabaseLockMode = Raven.Client.ServerWide.DatabaseLockMode;

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

export const openCreateDatabaseDialog = () => () => {
    const createDbView = new createDatabase("newDatabase");
    app.showBootstrapDialog(createDbView);
};

export const openCreateDatabaseFromRestoreDialog = () => () => {
    const createDbView = new createDatabase("restore");
    app.showBootstrapDialog(createDbView);
};

export const openDeleteDatabasesDialog =
    (toDelete: DatabaseSharedInfo[]): AppAsyncThunk =>
    async (dispatch, getState, getServices) => {
        const { databasesService } = getServices();

        const selectedDatabasesWithoutLock = toDelete.filter((x) => x.lockMode === "Unlock");
        if (selectedDatabasesWithoutLock.length === 0) {
            return;
        }

        const confirmDeleteViewModel = new deleteDatabaseConfirm(selectedDatabasesWithoutLock);
        confirmDeleteViewModel.result.done((confirmResult: deleteDatabaseConfirmResult) => {
            if (confirmResult.can) {
                /* TODO:
                const dbsList = toDelete.map(x => {
                    //TODO: x.isBeingDeleted(true);
                    const asDatabase = x.asDatabase();

                    // disconnect here to avoid race condition between database deleted message
                    // and websocket disconnection
                    //TODO: changesContext.default.disconnectIfCurrent(asDatabase, "DatabaseDeleted");
                    return asDatabase;
                });*/

                databasesService.deleteDatabase(
                    selectedDatabasesWithoutLock.map((x) => x.name),
                    !confirmResult.keepFiles
                );
            }
        });

        app.showBootstrapDialog(confirmDeleteViewModel);
    };

export const changeDatabasesLockMode =
    (databases: DatabaseSharedInfo[], lockMode: DatabaseLockMode): AppAsyncThunk =>
    async (dispatch, getState, getServices) => {
        const { databasesService } = getServices();

        await databasesService.setLockMode(databases, lockMode);
    };
