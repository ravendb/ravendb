import { configureStore, createListenerMiddleware, ListenerEffectAPI } from "@reduxjs/toolkit";
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
import { adminLogsMiddleware } from "components/pages/resources/manageServer/adminLogs/store/adminLogsMiddleware";
import { adminLogsSlice } from "components/pages/resources/manageServer/adminLogs/store/adminLogsSlice";
import { certificatesSlice } from "components/pages/resources/manageServer/certificates/store/certificatesSlice";
import { splitViewSlice } from "./common/splitView/store/splitViewSlice";
import { databaseMiddleware } from "components/common/shell/databaseMiddleware";
import { setupWizardSlice } from "./setupWizard/store/setupWizardSlice";
import { editGenAiTaskSlice } from "./pages/database/tasks/ongoingTasks/editTasks/editGenAiTask/store/editGenAiTaskSlice";
import { editAiAgentSlice } from "./pages/database/aiHub/aiAgents/edit/store/editAiAgentSlice";
import { chatAiAgentSlice } from "./pages/database/aiHub/aiAgents/chat/store/chatAiAgentSlice";
import { chatAiAgentUpdateUrlMiddleware } from "./pages/database/aiHub/aiAgents/chat/store/chatAiAgentMiddleware";
import { remoteAttachmentsSlice } from "./pages/database/settings/remoteAttachments/store/remoteAttachmentsSlice";
import { aiAssistantSlice } from "./common/shell/aiAssistantSlice";
import { chatbotSlice } from "./shell/chatbot/store/chatbotSlice";
import { chatbotMiddleware } from "./shell/chatbot/store/chatbotMiddleware";
import { documentSchemaSlice } from "components/pages/database/settings/documentSchema/store/documentSchemaSlice";
import { editCdcSinkTaskSlice } from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/store/editCdcSinkTaskSlice";

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
            adminLogs: adminLogsSlice.reducer,
            certificates: certificatesSlice.reducer,
            splitView: splitViewSlice.reducer,
            setupWizard: setupWizardSlice.reducer,
            editGenAiTask: editGenAiTaskSlice.reducer,
            editAiAgent: editAiAgentSlice.reducer,
            chatAiAgent: chatAiAgentSlice.reducer,
            remoteAttachments: remoteAttachmentsSlice.reducer,
            aiAssistant: aiAssistantSlice.reducer,
            chatbot: chatbotSlice.reducer,
            documentSchema: documentSchemaSlice.reducer,
            editCdcSinkTask: editCdcSinkTaskSlice.reducer,
        },
        middleware: (getDefaultMiddleware) =>
            getDefaultMiddleware({
                thunk: {
                    extraArgument: () => services,
                },
            })
                .prepend(listenerMiddleware.middleware)
                .prepend(connectionStringsUpdateUrlMiddleware.middleware)
                .prepend(databaseMiddleware.middleware)
                .prepend(adminLogsMiddleware.middleware)
                .prepend(chatAiAgentUpdateUrlMiddleware.middleware)
                .prepend(chatbotMiddleware.middleware),
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
export type AppListenerEffectApi = ListenerEffectAPI<RootState, AppDispatch, any>;

export default store;
