import { createListenerMiddleware } from "@reduxjs/toolkit";
import { connectionStringsActions } from "components/pages/database/settings/connectionStrings/store/connectionStringsSlice";
import activeDatabase from "common/shell/activeDatabaseTracker";
import appUrl from "common/appUrl";

export const connectionStringsUpdateUrlMiddleware = createListenerMiddleware();

connectionStringsUpdateUrlMiddleware.startListening({
    actionCreator: connectionStringsActions.editConnectionModalOpened,
    effect: (action) => {
        if (getIsServerWideConnectionStringsPage()) {
            const url = appUrl.forServerWideConnectionStrings(action.payload.type, action.payload.name);
            history.pushState(null, null, url);
            return;
        }

        if (getIsConnectionStringsPage()) {
            const url = appUrl.forConnectionStrings(
                activeDatabase.default.database(),
                action.payload.type,
                action.payload.name
            );
            history.pushState(null, null, url);
        }
    },
});

connectionStringsUpdateUrlMiddleware.startListening({
    actionCreator: connectionStringsActions.editConnectionModalClosed,
    effect: () => {
        if (getIsServerWideConnectionStringsPage()) {
            const url = appUrl.forServerWideConnectionStrings();
            history.pushState(null, null, url);
            return;
        }

        if (getIsConnectionStringsPage()) {
            const url = appUrl.forCurrentDatabase().connectionStrings();
            history.pushState(null, null, url);
        }
    },
});

const getIsConnectionStringsPage = () => window.location.href.includes("settings/connectionStrings");
const getIsServerWideConnectionStringsPage = () =>
    window.location.href.includes("settings/serverWideConnectionStrings");
