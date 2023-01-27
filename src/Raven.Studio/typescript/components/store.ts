import { configureStore } from "@reduxjs/toolkit";
import { TypedUseSelectorHook, useDispatch, useSelector } from "react-redux";
import { statisticsSlice } from "components/pages/database/status/statistics/logic/statisticsSlice";
import { BaseThunkAPI } from "@reduxjs/toolkit/dist/createAsyncThunk";
import { databasesSlice } from "components/common/shell/databasesSlice";

export function createStoreConfiguration() {
    return configureStore({
        reducer: {
            statistics: statisticsSlice.reducer,
            databases: databasesSlice.reducer,
        },
    });
}

const store = createStoreConfiguration();

export type RootState = ReturnType<typeof store.getState>;

export type AppDispatch = typeof store.dispatch;
export const useAppDispatch: () => AppDispatch = useDispatch;
export const useAppSelector: TypedUseSelectorHook<RootState> = useSelector;

export type AppAsyncThunk = (dispatch: AppDispatch, getState: () => RootState) => Promise<void>;

export type AppThunkApi = BaseThunkAPI<RootState, any, AppDispatch>;
export default store;
