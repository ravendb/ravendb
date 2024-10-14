import { DatabaseSharedInfo } from "components/models/databases";
import { AppAsyncThunk, AppDispatch, AppThunk } from "components/store";
import {
    databasesViewSlice,
    databasesViewSliceInternal,
} from "components/pages/resources/databases/store/databasesViewSlice";
import databasesManager from "common/shell/databasesManager";
import notificationCenter from "common/notifications/notificationCenter";
import DatabaseUtils from "components/utils/DatabaseUtils";
import { UnsubscribeListener } from "@reduxjs/toolkit";
import { clusterSelectors } from "components/common/shell/clusterSlice";
import app from "durandal/app";
import disableIndexingToggleConfirm from "viewmodels/resources/disableIndexingToggleConfirm";
import disableDatabaseToggleConfirm from "viewmodels/resources/disableDatabaseToggleConfirm";
import changesContext from "common/changesContext";
import compactDatabaseDialog from "viewmodels/resources/compactDatabaseDialog";
import DatabaseLockMode = Raven.Client.ServerWide.DatabaseLockMode;
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { addAppListener } from "components/storeUtils";
import { databasesSlice } from "components/common/shell/databasesSlice";

export const toggleIndexing =
    (db: DatabaseSharedInfo, disable: boolean): AppAsyncThunk =>
    async (dispatch, getState, getServices) => {
        const { indexesService } = getServices();

        if (disable) {
            await indexesService.disableAllIndexes(db.name);
            dispatch(databasesViewSlice.actions.disabledIndexing(db.name));
        } else {
            await indexesService.enableAllIndexes(db.name);
            dispatch(databasesViewSlice.actions.enabledIndexing(db.name));
        }
    };

export const openNotificationCenterForDatabase =
    (db: DatabaseSharedInfo): AppThunk =>
    (dispatch, getState) => {
        if (!db.currentNode.isRelevant) {
            return;
        }

        const activeDatabaseName = databaseSelectors.activeDatabaseName(getState());
        if (activeDatabaseName !== db.name) {
            const dbRaw = databasesManager.default.getDatabaseByName(db.name);
            if (dbRaw) {
                databasesManager.default.activate(dbRaw);
            }
        }

        notificationCenter.instance.showNotifications.toggle();
    };

export const togglePauseIndexing =
    (db: DatabaseSharedInfo, pause: boolean): AppAsyncThunk =>
    async (dispatch, getState, getServices) => {
        const { indexesService } = getServices();
        const locations = DatabaseUtils.getLocations(db);

        if (pause) {
            const tasks = locations.map(async (l) => {
                await indexesService.pauseAllIndexes(db.name, l);
                dispatch(databasesViewSlice.actions.pausedIndexing(db.name, l));
            });
            await Promise.all(tasks);
        } else {
            const tasks = locations.map(async (l) => {
                await indexesService.resumeAllIndexes(db.name, l);
                dispatch(databasesViewSlice.actions.resumedIndexing(db.name, l));
            });
            await Promise.all(tasks);
        }
    };

export const syncDatabaseDetails = (): AppThunk<UnsubscribeListener> => (dispatch) => {
    return dispatch(
        addAppListener({
            actionCreator: databasesSlice.actions.databasesLoaded,
            effect: (_, api) => {
                api.dispatch(throttledReloadDatabaseDetails);
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
    async (dispatch, getState) => {
        const nodeTags = clusterSelectors.allNodeTags(getState());
        const tasks = nodeTags.map((nodeTag) =>
            dispatch(databasesViewSliceInternal.fetchDatabase({ nodeTag, databaseName }))
        );
        await Promise.all(tasks);
    };

export const reloadDatabasesDetails: AppAsyncThunk = async (dispatch, getState) => {
    const state = getState();
    const nodeTags = clusterSelectors.allNodeTags(state);

    const tasks = nodeTags.map((nodeTag) => dispatch(databasesViewSliceInternal.fetchDatabases(nodeTag)));

    await Promise.all(tasks);
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
        await databasesService.toggle(
            databases.map((x) => x.name),
            enable
        );
    };

export const throttledReloadDatabaseDetails = _.throttle(reloadDatabasesDetails, 100);

export const changeDatabasesLockMode =
    (databases: DatabaseSharedInfo[], lockMode: DatabaseLockMode): AppAsyncThunk =>
    async (dispatch, getState, getServices) => {
        const { databasesService } = getServices();

        await databasesService.setLockMode(
            databases.map((x) => x.name),
            lockMode
        );
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

export const compactDatabase = (database: DatabaseSharedInfo, shardNumber?: number) => () => {
    const db = databasesManager.default.getDatabaseByName(database.name);
    if (db) {
        changesContext.default.disconnectIfCurrent(db, "DatabaseDisabled");
    }
    app.showBootstrapDialog(new compactDatabaseDialog(database, shardNumber));
};
