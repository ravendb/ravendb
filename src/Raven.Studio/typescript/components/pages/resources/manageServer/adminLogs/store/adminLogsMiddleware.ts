import { createListenerMiddleware, isAnyOf } from "@reduxjs/toolkit";
import { adminLogsActions, AdminLogsMessage } from "./adminLogsSlice";
import { AppListenerEffectApi } from "components/store";
import adminLogsWebSocketClient from "common/adminLogsWebSocketClient";
import eventsCollector from "common/eventsCollector";

export const adminLogsMiddleware = createListenerMiddleware();

let liveClient: adminLogsWebSocketClient = null;
let pushLogsBatchesInterval: NodeJS.Timeout = null;

adminLogsMiddleware.startListening({
    matcher: isAnyOf(adminLogsActions.isPausedSet, adminLogsActions.isPausedToggled),
    effect: (_, listenerApi: AppListenerEffectApi) => {
        const state = listenerApi.getState();

        if (state.adminLogs.isPaused) {
            listenerApi.dispatch(adminLogsActions.liveClientStopped());
        } else if (!liveClient) {
            listenerApi.dispatch(adminLogsActions.liveClientStarted());
        }
    },
});

adminLogsMiddleware.startListening({
    actionCreator: adminLogsActions.liveClientStopped,
    effect: () => {
        liveClient?.dispose();
        liveClient = null;
        clearInterval(pushLogsBatchesInterval);
        pushLogsBatchesInterval = null;
    },
});

adminLogsMiddleware.startListening({
    actionCreator: adminLogsActions.liveClientStarted,
    effect: (_, listenerApi: AppListenerEffectApi) => {
        if (liveClient) {
            return;
        }

        eventsCollector.default.reportEvent("admin-logs", "connect");

        let logsBatch: Omit<AdminLogsMessage, "_meta">[] = [];

        pushLogsBatchesInterval = setInterval(() => {
            if (logsBatch.length === 0) {
                return;
            }

            listenerApi.dispatch(adminLogsActions.logsManyAppended(logsBatch));
            logsBatch = [];
        }, 500);

        liveClient = new adminLogsWebSocketClient((message) => {
            const adminLogsState = listenerApi.getState().adminLogs;

            if (adminLogsState.logs.length + logsBatch.length >= adminLogsState.maxLogsCount) {
                listenerApi.dispatch(adminLogsActions.logsManyAppended(logsBatch));
                logsBatch = [];
                listenerApi.dispatch(adminLogsActions.isPausedSet(true));
                listenerApi.dispatch(adminLogsActions.isBufferFullAlertOpenSet(true));
                return;
            }

            logsBatch.push(message);
        });
    },
});

adminLogsMiddleware.startListening({
    matcher: isAnyOf(adminLogsActions.maxLogsCountSet, adminLogsActions.logsManyAppended, adminLogsActions.logsSet),
    effect: (_, listenerApi: AppListenerEffectApi) => {
        const state = listenerApi.getState();

        if (state.adminLogs.logs.length >= state.adminLogs.maxLogsCount) {
            listenerApi.dispatch(adminLogsActions.isPausedSet(true));
            listenerApi.dispatch(adminLogsActions.isBufferFullAlertOpenSet(true));
        } else {
            listenerApi.dispatch(adminLogsActions.isBufferFullAlertOpenSet(false));
        }
    },
});

adminLogsMiddleware.startListening({
    actionCreator: adminLogsActions.isAllExpandedToggled,
    effect: (_, listenerApi: AppListenerEffectApi) => {
        const state = listenerApi.getState();

        listenerApi.dispatch(
            adminLogsActions.logsSet(
                state.adminLogs.logs.map((log) => ({
                    ...log,
                    _meta: { ...log._meta, isExpanded: state.adminLogs.isAllExpanded },
                }))
            )
        );
    },
});
