import { chatbotActions, ChatbotAttachedContext, ChatbotMessage } from "components/shell/chatbot/store/chatbotSlice";
import { globalDispatch } from "components/storeCompat";

export class MockChatbot {
    with_isOpen(isOpen: boolean) {
        globalDispatch(chatbotActions.isOpenSet(isOpen));
    }

    with_messages(messages: ChatbotMessage[]) {
        globalDispatch(chatbotActions.messagesSet(messages));
    }

    with_attachedContextUpserted(context: ChatbotAttachedContext) {
        globalDispatch(chatbotActions.attachedContextUpserted(context));
    }
}
