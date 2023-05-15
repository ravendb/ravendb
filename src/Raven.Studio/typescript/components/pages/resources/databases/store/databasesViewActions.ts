import { DatabaseSharedInfo } from "components/models/databases";
import { AppAsyncThunk, AppDispatch, AppThunk } from "components/store";
import {
    databasesViewSlice,
    databasesViewSliceInternal,
} from "components/pages/resources/databases/store/databasesViewSlice";
import databasesManager from "common/shell/databasesManager";
import notificationCenter from "common/notifications/notificationCenter";
import viewHelpers from "common/helpers/view/viewHelpers";
import DatabaseUtils from "components/utils/DatabaseUtils";
import { UnsubscribeListener } from "@reduxjs/toolkit";
import { addAppListener } from "components/storeUtils";
import { databasesSlice } from "components/common/shell/databasesSlice";
import { clusterSelectors } from "components/common/shell/clusterSlice";
import createDatabase from "viewmodels/resources/createDatabase";
import app from "durandal/app";
import disableIndexingToggleConfirm from "viewmodels/resources/disableIndexingToggleConfirm";
import disableDatabaseToggleConfirm from "viewmodels/resources/disableDatabaseToggleConfirm";
import changesContext from "common/changesContext";
import compactDatabaseDialog from "viewmodels/resources/compactDatabaseDialog";
import DatabaseLockMode = Raven.Client.ServerWide.DatabaseLockMode;
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";

export const toggleIndexing =
    (db: DatabaseSharedInfo, disable: boolean): AppAsyncThunk =>
    async (dispatch, getState, getServices) => {
        const { indexesService } = getServices();

        if (disable) {
            await indexesService.disableAllIndexes(db);
            dispatch(databasesViewSlice.actions.disabledIndexing(db.name));
        } else {
            await indexesService.enableAllIndexes(db);
            dispatch(databasesViewSlice.actions.enabledIndexing(db.name));
        }
    };

export const openNotificationCenterForDatabase =
    (db: DatabaseSharedInfo): AppThunk =>
    (dispatch, getState) => {
        const activeDatabase = databaseSelectors.activeDatabase(getState());
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
                dispatch(databasesViewSlice.actions.pausedIndexing(db.name, l));
            });
            await Promise.all(tasks);
        } else {
            const tasks = locations.map(async (l) => {
                await indexesService.resumeAllIndexes(db, l);
                dispatch(databasesViewSlice.actions.resumedIndexing(db.name, l));
            });
            await Promise.all(tasks);
        }
    };

export const syncDatabaseDetails = (): AppThunk<UnsubscribeListener> => (dispatch) => {
    return dispatch(
        addAppListener({
            actionCreator: databasesSlice.actions.databasesLoaded,
            effect: (action, api) => {
                const state = api.getState();
                const existingData = state.databasesView.databaseDetailedInfo.ids;

                const needsRefresh = action.payload.some((db) => {
                    const locations = DatabaseUtils.getLocations(db);
                    const ids = locations.map((l) => databasesViewSliceInternal.selectDatabaseInfoId(db.name, l));
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
    dispatch(databasesViewSlice.actions.initDetails(nodeTags));

    const tasks = nodeTags.map((nodeTag) => dispatch(databasesViewSliceInternal.fetchDatabases(nodeTag)));

    await Promise.all(tasks);
};

export const reloadDatabaseDetails =
    (databaseName: string): AppAsyncThunk =>
    async (dispatch: AppDispatch, getState) => {
        const nodeTags = clusterSelectors.allNodeTags(getState());
        const tasks = nodeTags.map((nodeTag) =>
            dispatch(databasesViewSliceInternal.fetchDatabase({ nodeTag, databaseName }))
        );
        await Promise.all(tasks);
    };

export const confirmSetLockMode = (): AppAsyncThunk<boolean> => async () => {
    const result = await viewHelpers.confirmationMessage("Are you sure?", `Do you want to change lock mode?`);

    return result.can;
};

export const reloadDatabasesDetails: AppAsyncThunk = async (dispatch: AppDispatch, getState) => {
    const state = getState();
    const nodeTags = clusterSelectors.allNodeTags(state);

    const tasks = nodeTags.map((nodeTag) => dispatch(databasesViewSliceInternal.fetchDatabases(nodeTag)));

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

export const throttledReloadDatabaseDetails = _.throttle(reloadDatabasesDetails, 100);

export const changeDatabasesLockMode =
    (databases: DatabaseSharedInfo[], lockMode: DatabaseLockMode): AppAsyncThunk =>
    async (dispatch, getState, getServices) => {
        const { databasesService } = getServices();

        await databasesService.setLockMode(databases, lockMode);
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
    }*/

export const compactDatabase = (database: DatabaseSharedInfo) => () => {
    const db = databasesManager.default.getDatabaseByName(database.name);
    if (db) {
        changesContext.default.disconnectIfCurrent(db, "DatabaseDisabled");
    }
    app.showBootstrapDialog(new compactDatabaseDialog(database));
};

