import { createEntityAdapter, createSlice, EntityState, PayloadAction } from "@reduxjs/toolkit";
import { DatabaseSharedInfo } from "components/models/databases";
import genUtils from "common/generalUtils";
import { AppAsyncThunk, RootState } from "components/store";
import createDatabase from "viewmodels/resources/createDatabase";
import app from "durandal/app";
import deleteDatabaseConfirm from "viewmodels/resources/deleteDatabaseConfirm";
import DatabaseLockMode = Raven.Client.ServerWide.DatabaseLockMode;
import disableDatabaseToggleConfirm from "viewmodels/resources/disableDatabaseToggleConfirm";
import viewHelpers from "common/helpers/view/viewHelpers";

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

//TODO: report success after database deletion? - what about other actions?
export const confirmDeleteDatabases =
    (
        toDelete: DatabaseSharedInfo[]
    ): AppAsyncThunk<{ can: boolean; keepFiles?: boolean; databases?: DatabaseSharedInfo[] }> =>
    async (): Promise<{ can: boolean; keepFiles?: boolean; databases?: DatabaseSharedInfo[] }> => {
        const selectedDatabasesWithoutLock = toDelete.filter((x) => x.lockMode === "Unlock");
        if (selectedDatabasesWithoutLock.length === 0) {
            return {
                can: false,
            };
        }

        const confirmDeleteViewModel = new deleteDatabaseConfirm(selectedDatabasesWithoutLock);
        app.showBootstrapDialog(confirmDeleteViewModel);
        const baseResult = await confirmDeleteViewModel.result;
        return {
            ...baseResult,
            databases: selectedDatabasesWithoutLock,
        };
    };

export const deleteDatabases =
    (toDelete: DatabaseSharedInfo[], keepFiles: boolean): AppAsyncThunk<updateDatabaseConfigurationsResult> =>
    async (dispatch, getState, getServices) => {
        const { databasesService } = getServices();
        /* TODO:
           const dbsList = toDelete.map(x => {
               //TODO: x.isBeingDeleted(true);
               const asDatabase = x.asDatabase();

               // disconnect here to avoid race condition between database deleted message
               // and websocket disconnection
               //TODO: changesContext.default.disconnectIfCurrent(asDatabase, "DatabaseDeleted");
               return asDatabase;
           });*/

        return databasesService.deleteDatabase(
            toDelete.map((x) => x.name),
            !keepFiles
        );
    };

export const changeDatabasesLockMode =
    (databases: DatabaseSharedInfo[], lockMode: DatabaseLockMode): AppAsyncThunk =>
    async (dispatch, getState, getServices) => {
        const { databasesService } = getServices();

        await databasesService.setLockMode(databases, lockMode);
    };

export const confirmToggleDatabases =
    (databases: DatabaseSharedInfo[], enable: boolean): AppAsyncThunk<boolean> =>
    async () => {
        const confirmation = new disableDatabaseToggleConfirm(databases, !enable);
        app.showBootstrapDialog(confirmation);

        const result = await confirmation.result;
        return result.can;
    };

export const toggleDatabases =
    (databases: DatabaseSharedInfo[], enable: boolean): AppAsyncThunk =>
    async (dispatch, getState, getServices) => {
        const { databasesService } = getServices();
        // TODO: lazy update UI
        await databasesService.toggle(databases, enable);
    };

/* TODO
    private onDatabaseDisabled(result: disableDatabaseResult) {
        const dbs = this.databases().sortedDatabases();
        const matchedDatabase = dbs.find(rs => rs.name === result.Name);

        if (matchedDatabase) {
            matchedDatabase.disabled(result.Disabled);

            // If Enabling a database (that is selected from the top) than we want it to be Online(Loaded)
            if (matchedDatabase.isCurrentlyActiveDatabase() && !matchedDatabase.disabled()) {
                new loadDatabaseCommand(matchedDatabase.asDatabase())
                    .execute();
            }
        }
    }
 */

export const confirmSetLockMode = (): AppAsyncThunk<boolean> => async () => {
    const result = await viewHelpers.confirmationMessage("Are you sure?", `Do you want to change lock mode?`);

    return result.can;
};
