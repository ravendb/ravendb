import { createListenerMiddleware, isAnyOf } from "@reduxjs/toolkit";
import { chatbotActions } from "./chatbotSlice";
import { RootState } from "components/store";
import studioSettings from "common/settings/studioSettings";

export const chatbotMiddleware = createListenerMiddleware();

chatbotMiddleware.startListening({
    matcher: isAnyOf(chatbotActions.isOpenToggled, chatbotActions.isOpenSet),
    effect: (_, { getState }) => {
        const state = getState() as RootState;
        toggleLayoutClass("show-chatbot", state.chatbot.isOpen);
    },
});

chatbotMiddleware.startListening({
    actionCreator: chatbotActions.isPinnedToggled,
    effect: (_, { getState }) => {
        const state = getState() as RootState;
        toggleLayoutClass("pin-chatbot", state.chatbot.isPinned);
    },
});

chatbotMiddleware.startListening({
    actionCreator: chatbotActions.isAlwaysAllowEndpointCallsSet,
    effect: async ({ payload }) => {
        const globalSettings = await studioSettings.default.globalSettings();
        globalSettings.isChatbotAlwaysAllowEndpointCalls.setValue(payload);
    },
});

chatbotMiddleware.startListening({
    actionCreator: chatbotActions.isDataSubmissionEnabledSet,
    effect: async ({ payload }, { dispatch }) => {
        const globalSettings = await studioSettings.default.globalSettings();
        globalSettings.isChatbotDataSubmissionEnabled.setValue(payload);

        if (!payload) {
            dispatch(chatbotActions.attachedContextTypesRemoved(["QueryResult"]));
        }
    },
});

function toggleLayoutClass(className: string, isActive: boolean) {
    document.querySelector(".layout-container").classList.toggle(className, isActive);
}
