import { createListenerMiddleware } from "@reduxjs/toolkit";
import { accessManagerSelectors } from "components/common/shell/accessManagerSliceSelectors";
import { databaseActions } from "components/common/shell/databaseSliceActions";
import { services } from "components/hooks/useServices";
import { RootState } from "components/store";
import { settingsEntry } from "models/database/settings/databaseSettingsModels";

export const databaseMiddleware = createListenerMiddleware();

databaseMiddleware.startListening({
    actionCreator: databaseActions.activeDatabaseChanged,
    effect: async (action, listenerApi) => {
        const dbName = action.payload;
        const state = listenerApi.getState() as RootState;

        if (!dbName || !accessManagerSelectors.getHasDatabaseAdminAccess(state)(dbName)) {
            listenerApi.dispatch(databaseActions.activeDatabaseSettingsLoaded({}));
            return;
        }

        try {
            const result = await services.databasesService.getDatabaseSettings(dbName);
            const settingsEntries = result.Settings.map(settingsEntry.getEntry);

            const settingsRecord: Record<string, string> = {};
            settingsEntries.forEach((entry) => {
                settingsRecord[entry.keyName()] = entry.serverOrDefaultValue();
            });

            listenerApi.dispatch(databaseActions.activeDatabaseSettingsLoaded(settingsRecord));
        } catch {
            listenerApi.dispatch(databaseActions.activeDatabaseSettingsLoaded({}));
        }
    },
});
