import { useAppDispatch, useAppSelector } from "components/store";
import { chatbotSelectors, chatbotActions } from "../store/chatbotSlice";
import ChatbotMessages from "./ChatbotMessages";
import AiAssistantConsentStatusChecker from "components/common/aiAssistant/AiAssistantConsentStatusChecker";
import ChatbotAskAiWelcome from "./askAi/ChatbotAskAiWelcome";
import ChatbotAskAiPromptPanel from "components/shell/chatbot/partials/askAi/ChatbotAskAiPromptPanel";

export default function ChatbotPanelAskAi() {
    const dispatch = useAppDispatch();
    const lastRunData = useAppSelector(chatbotSelectors.lastRunData);

    const onConsentGiven = () => {
        if (lastRunData) {
            dispatch(chatbotActions.retryRunChat());
        }
    };

    return (
        <div className="vstack flex-grow py-2 h-100">
            <div className="overflow-y-auto">
                <AiAssistantConsentStatusChecker className="p-2 flex-grow" onConsentGiven={onConsentGiven} />
                <ChatbotAskAiWelcome />
            </div>
            <ChatbotMessages />
            <ChatbotAskAiPromptPanel />
        </div>
    );
}
