import { configureStore, createListenerMiddleware } from "@reduxjs/toolkit";
import { TypedUseSelectorHook, useDispatch, useSelector } from "react-redux";
import { statisticsViewSlice } from "components/pages/database/status/statistics/store/statisticsViewSlice";
import { GetThunkAPI } from "@reduxjs/toolkit";
import { databasesSlice } from "components/common/shell/databasesSlice";
import { services } from "hooks/useServices";
import { accessManagerSlice } from "components/common/shell/accessManagerSlice";
import { clusterSlice } from "components/common/shell/clusterSlice";
import { databasesViewSlice } from "components/pages/resources/databases/store/databasesViewSlice";
import { licenseSlice } from "./common/shell/licenseSlice";
import { documentRevisionsSlice } from "./pages/database/settings/documentRevisions/store/documentRevisionsSlice";
import { collectionsTrackerSlice } from "./common/shell/collectionsTrackerSlice";
import { conflictResolutionSlice } from "./pages/database/settings/conflictResolution/store/conflictResolutionSlice";
import { connectionStringsSlice } from "./pages/database/settings/connectionStrings/store/connectionStringsSlice";
import { connectionStringsUpdateUrlMiddleware } from "./pages/database/settings/connectionStrings/store/connectionStringsMiddleware";
import { certificatesSlice } from "components/pages/resources/manageServer/certificates/store/certificatesSlice";

const listenerMiddleware = createListenerMiddleware({
    extra: () => services,
});

export function createStoreConfiguration() {
    return configureStore({
        reducer: {
            statistics: statisticsViewSlice.reducer,
            databases: databasesSlice.reducer,
            databasesView: databasesViewSlice.reducer,
            accessManager: accessManagerSlice.reducer,
            cluster: clusterSlice.reducer,
            license: licenseSlice.reducer,
            documentRevisions: documentRevisionsSlice.reducer,
            collectionsTracker: collectionsTrackerSlice.reducer,
            conflictResolution: conflictResolutionSlice.reducer,
            connectionStrings: connectionStringsSlice.reducer,
            certificates: certificatesSlice.reducer,
        },
        middleware: (getDefaultMiddleware) =>
            getDefaultMiddleware({
                thunk: {
                    extraArgument: () => services,
                },
            })
                .prepend(listenerMiddleware.middleware)
                .prepend(connectionStringsUpdateUrlMiddleware.middleware),
    });
}

const store = createStoreConfiguration();

export type RootState = ReturnType<typeof store.getState>;

export type AppDispatch = typeof store.dispatch;
export const useAppDispatch: () => AppDispatch = useDispatch;
export const useAppSelector: TypedUseSelectorHook<RootState> = useSelector;

export type AppAsyncThunk<T = void> = (
    dispatch: AppDispatch,
    getState: () => RootState,
    getServices: () => typeof services
) => Promise<T>;

export type AppThunk<T = void> = (
    dispatch: AppDispatch,
    getState: () => RootState,
    getServices: () => typeof services
) => T;

export type AppThunkApi = GetThunkAPI<{
    state: RootState;
    dispatch: AppDispatch;
    extra: () => typeof services;
    rejectValue: unknown;
}>;
export default store;
