import { createListenerMiddleware } from "@reduxjs/toolkit";
import appUrl from "common/appUrl";
import { chatAiAgentActions } from "./chatAiAgentSlice";
import { RootState } from "components/store";

export const chatAiAgentUpdateUrlMiddleware = createListenerMiddleware();

chatAiAgentUpdateUrlMiddleware.startListening({
    actionCreator: chatAiAgentActions.conversationIdSet,
    effect: (action, { getState }) => {
        if (!action.payload) {
            return;
        }

        const state = getState() as RootState;

        const databaseName = state.databases.activeDatabaseName;
        const agentId = state.chatAiAgent.config.data?.Identifier;

        const url = appUrl.forChatAiAgent(databaseName, agentId, action.payload);

        history.pushState(null, null, url);
    },
});
