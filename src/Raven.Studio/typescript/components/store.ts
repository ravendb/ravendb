import { configureStore, createListenerMiddleware } from "@reduxjs/toolkit";
import { TypedUseSelectorHook, useDispatch, useSelector } from "react-redux";
import { statisticsViewSlice } from "components/pages/database/status/statistics/store/statisticsViewSlice";
import { BaseThunkAPI } from "@reduxjs/toolkit/dist/createAsyncThunk";
import { databasesSlice } from "components/common/shell/databasesSlice";
import { services } from "hooks/useServices";
import { accessManagerSlice } from "components/common/shell/accessManagerSlice";
import { clusterSlice } from "components/common/shell/clusterSlice";
import { databasesViewSlice } from "components/pages/resources/databases/store/databasesViewSlice";
import { licenseSlice } from "./common/shell/licenseSlice";

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
        },
        middleware: (getDefaultMiddleware) =>
            getDefaultMiddleware({
                thunk: {
                    extraArgument: () => services,
                },
            }).prepend(listenerMiddleware.middleware),
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

export type AppThunkApi = BaseThunkAPI<RootState, any, AppDispatch>;
export default store;
