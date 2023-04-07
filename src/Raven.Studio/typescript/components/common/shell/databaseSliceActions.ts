import { DatabaseSharedInfo } from "components/models/databases";
import { AppAsyncThunk, AppDispatch, AppThunk } from "components/store";
import databasesManager from "common/shell/databasesManager";
import notificationCenter from "common/notifications/notificationCenter";
import viewHelpers from "common/helpers/view/viewHelpers";
import DatabaseUtils from "components/utils/DatabaseUtils";
import deleteDatabaseConfirm from "viewmodels/resources/deleteDatabaseConfirm";
import app from "durandal/app";
import changesContext from "common/changesContext";
import compactDatabaseDialog from "viewmodels/resources/compactDatabaseDialog";
import disableDatabaseToggleConfirm from "viewmodels/resources/disableDatabaseToggleConfirm";
import { UnsubscribeListener } from "@reduxjs/toolkit";
import { addAppListener } from "components/storeUtils";
import { databasesSlice, databasesSliceInternal } from "components/common/shell/databasesSlice";
import { selectActiveDatabase } from "components/common/shell/databaseSliceSelectors";
import DatabaseLockMode = Raven.Client.ServerWide.DatabaseLockMode;
import { selectClusterNodeTags } from "components/common/shell/clusterSlice";
import createDatabase from "viewmodels/resources/createDatabase";
import disableIndexingToggleConfirm from "viewmodels/resources/disableIndexingToggleConfirm";

export const toggleIndexing =
    (db: DatabaseSharedInfo, disable: boolean): AppAsyncThunk =>
    async (dispatch, getState, getServices) => {
        const { indexesService } = getServices();

        if (disable) {
            await indexesService.disableAllIndexes(db);
            dispatch(databasesSlice.actions.disabledIndexing(db.name));
        } else {
            await indexesService.enableAllIndexes(db);
            dispatch(databasesSlice.actions.enabledIndexing(db.name));
        }
    };

export const openNotificationCenterForDatabase =
    (db: DatabaseSharedInfo): AppThunk =>
    (dispatch, getState) => {
        const activeDatabase = selectActiveDatabase(getState());
        if (activeDatabase !== db.name) {
            const dbRaw = databasesManager.default.getDatabaseByName(db.name);
            if (dbRaw) {
                databasesManager.default.activate(dbRaw);
            }
        }

        notificationCenter.instance.showNotifications.toggle();
    };

export const confirmTogglePauseIndexing =
    (db: DatabaseSharedInfo, pause: boolean): AppAsyncThunk<{ can: boolean; locations: databaseLocationSpecifier[] }> =>
    async () => {
        //TODO: context selector!
        const msg = pause ? "pause indexing?" : "resume indexing?";
        const result = await viewHelpers.confirmationMessage("Are you sure?", `Do you want to ` + msg);

        return {
            can: result.can,
            locations: DatabaseUtils.getLocations(db),
        };
    };

export const togglePauseIndexing =
    (db: DatabaseSharedInfo, pause: boolean, locations: databaseLocationSpecifier[]): AppAsyncThunk =>
    async (dispatch, getState, getServices) => {
        const { indexesService } = getServices();

        if (pause) {
            const tasks = locations.map(async (l) => {
                await indexesService.pauseAllIndexes(db, l);
                dispatch(databasesSlice.actions.pausedIndexing(db.name, l));
            });
            await Promise.all(tasks);
        } else {
            const tasks = locations.map(async (l) => {
                await indexesService.resumeAllIndexes(db, l);
                dispatch(databasesSlice.actions.resumedIndexing(db.name, l));
            });
            await Promise.all(tasks);
        }
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

export const compactDatabase = (database: DatabaseSharedInfo) => () => {
    const db = databasesManager.default.getDatabaseByName(database.name);
    if (db) {
        changesContext.default.disconnectIfCurrent(db, "DatabaseDisabled");
    }
    app.showBootstrapDialog(new compactDatabaseDialog(database));
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

export const syncDatabaseDetails = (): AppThunk<UnsubscribeListener> => (dispatch) => {
    return dispatch(
        addAppListener({
            actionCreator: databasesSlice.actions.databasesLoaded,
            effect: (action, api) => {
                const state = api.getState();
                const existingData = state.databases.localDatabaseDetailedInfo.ids;

                const needsRefresh = action.payload.some((db) => {
                    const locations = DatabaseUtils.getLocations(db);
                    const ids = locations.map((l) => databasesSliceInternal.selectDatabaseInfoId(db.name, l));
                    return ids.some((id) => !existingData.includes(id));
                });

                if (needsRefresh) {
                    api.dispatch(throttledReloadDatabaseDetails);
                }
            },
        })
    );
};

export const loadDatabasesDetails = (nodeTags: string[]) => async (dispatch: AppDispatch) => {
    dispatch(initDetails(nodeTags));

    const tasks = nodeTags.map((nodeTag) => dispatch(databasesSliceInternal.fetchDatabases(nodeTag)));

    await Promise.all(tasks);
};

export const reloadDatabaseDetails =
    (databaseName: string): AppAsyncThunk =>
    async (dispatch: AppDispatch, getState) => {
        const nodeTags = selectClusterNodeTags(getState());
        const tasks = nodeTags.map((nodeTag) =>
            dispatch(databasesSliceInternal.fetchDatabase({ nodeTag, databaseName }))
        );
        await Promise.all(tasks);
    };

export const reloadDatabasesDetails: AppAsyncThunk = async (dispatch: AppDispatch, getState) => {
    const state = getState();
    const nodeTags = selectClusterNodeTags(state);

    const tasks = nodeTags.map((nodeTag) => dispatch(databasesSliceInternal.fetchDatabases(nodeTag)));

    await Promise.all(tasks);
};

export const openCreateDatabaseDialog = () => () => {
    const createDbView = new createDatabase("newDatabase");
    app.showBootstrapDialog(createDbView);
};

export const openCreateDatabaseFromRestoreDialog = () => () => {
    const createDbView = new createDatabase("restore");
    app.showBootstrapDialog(createDbView);
};

export const confirmToggleIndexing =
    (db: DatabaseSharedInfo, disable: boolean): AppAsyncThunk<{ can: boolean }> =>
    async () => {
        const confirmDeleteViewModel = new disableIndexingToggleConfirm(disable);
        app.showBootstrapDialog(confirmDeleteViewModel);
        return confirmDeleteViewModel.result;
    };

export const throttledReloadDatabaseDetails = _.throttle(reloadDatabasesDetails, 100);

export const { activeDatabaseChanged, databasesLoaded, initDetails } = databasesSlice.actions;
