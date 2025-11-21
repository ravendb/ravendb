import { chatbotActions, ChatbotAttachedContextId, ChatbotMessage } from "components/shell/chatbot/store/chatbotSlice";
import { globalDispatch } from "components/storeCompat";

export class MockChatbot {
    with_isOpen(isOpen: boolean) {
        globalDispatch(chatbotActions.isOpenSet(isOpen));
    }

    with_messages(messages: ChatbotMessage[]) {
        globalDispatch(chatbotActions.messagesSet(messages));
    }

    with_attachedContextSet(context: { id: ChatbotAttachedContextId; label: string; value: string }) {
        globalDispatch(chatbotActions.attachedContextSet(context));
    }
}
