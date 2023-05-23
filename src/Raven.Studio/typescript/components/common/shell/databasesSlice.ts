import { createEntityAdapter, createSlice, EntityState, PayloadAction } from "@reduxjs/toolkit";
import { DatabaseLocalInfo, DatabaseSharedInfo } from "components/models/databases";
import genUtils from "common/generalUtils";
import DatabaseUtils from "components/utils/DatabaseUtils";
import StudioDatabaseState = Raven.Server.Web.System.Processors.Studio.StudioDatabasesHandlerForGetDatabasesState.StudioDatabaseState;

export interface DatabasesState {
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

export function toDatabaseLocalInfo(db: StudioDatabaseState, nodeTag: string): DatabaseLocalInfo {
    return {
        name: DatabaseUtils.shardGroupKey(db.Name),
        location: {
            nodeTag,
            shardNumber: DatabaseUtils.shardNumber(db.Name),
        },
        alerts: db.Alerts,
        loadError: db.LoadError,
        documentsCount: db.DocumentsCount,
        indexingStatus: db.IndexingStatus,
        indexingErrors: db.IndexingErrors,
        performanceHints: db.PerformanceHints,
        upTime: db.UpTime ? genUtils.timeSpanAsAgo(db.UpTime, false) : null, // we format here to avoid constant updates of UI
        backupInfo: db.BackupInfo,
        totalSize: db.TotalSize,
        tempBuffersSize: db.TempBuffersSize,
        databaseStatus: db.DatabaseStatus,
    };
}

export const databasesSliceInternal = {
    databasesSelectors,
};
