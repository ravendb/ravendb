import { createListenerMiddleware, isAnyOf } from "@reduxjs/toolkit";
import { chatbotActions } from "./chatbotSlice";
import { RootState } from "components/store";

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

function toggleLayoutClass(className: string, isActive: boolean) {
    document.querySelector(".layout-container").classList.toggle(className, isActive);
}
