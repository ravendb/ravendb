import { createListenerMiddleware } from "@reduxjs/toolkit";
import { connectionStringsActions } from "components/pages/database/settings/connectionStrings/store/connectionStringsSlice";
import activeDatabase from "common/shell/activeDatabaseTracker";
import appUrl from "common/appUrl";

export const connectionStringsUpdateUrlMiddleware = createListenerMiddleware();

connectionStringsUpdateUrlMiddleware.startListening({
    actionCreator: connectionStringsActions.editConnectionModalOpened,
    effect: (action) => {
        if (!getIsConnectionStringsPage()) {
            return;
        }

        const url = appUrl.forConnectionStrings(
            activeDatabase.default.database(),
            action.payload.type,
            action.payload.name
        );

        history.pushState(null, null, url);
    },
});

connectionStringsUpdateUrlMiddleware.startListening({
    actionCreator: connectionStringsActions.editConnectionModalClosed,
    effect: () => {
        if (!getIsConnectionStringsPage()) {
            return;
        }

        const url = appUrl.forCurrentDatabase().connectionStrings();
        history.pushState(null, null, url);
    },
});

const getIsConnectionStringsPage = () => window.location.href.includes("settings/connectionStrings");
