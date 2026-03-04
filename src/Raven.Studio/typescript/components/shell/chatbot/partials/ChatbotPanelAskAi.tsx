import { useAppDispatch, useAppSelector } from "components/store";
import { chatbotSelectors, chatbotActions } from "../store/chatbotSlice";
import ChatbotMessages from "./ChatbotMessages";
import AiAssistantConsentStatusChecker from "components/common/aiAssistant/AiAssistantConsentStatusChecker";
import ChatbotAskAiWelcome from "./askAi/ChatbotAskAiWelcome";
import ChatbotAskAiPromptPanel from "components/shell/chatbot/partials/askAi/ChatbotAskAiPromptPanel";
import classNames from "classnames";
import { aiAssistantSelectors } from "components/common/shell/aiAssistantSlice";

export default function ChatbotPanelAskAi() {
    const dispatch = useAppDispatch();
    const lastRunData = useAppSelector(chatbotSelectors.lastRunData);
    const messagesCount = useAppSelector(chatbotSelectors.messagesCount);
    const isConsentSuccess = useAppSelector(aiAssistantSelectors.isConsentSuccess);

    const onConsentGiven = () => {
        if (lastRunData) {
            dispatch(chatbotActions.retryRunChat());
        }
    };

    return (
        <div className="vstack flex-grow py-2 h-100">
            <div
                className={classNames(
                    "overflow-y-auto d-flex h-100 justify-content-center align-items-center",
                    messagesCount !== 0 && "d-none"
                )}
            >
                <AiAssistantConsentStatusChecker
                    className="p-2 flex-grow"
                    onConsentGiven={onConsentGiven}
                    hasAsciiIcon
                />
                <ChatbotAskAiWelcome />
            </div>
            {isConsentSuccess && (
                <>
                    <ChatbotMessages />
                    <ChatbotAskAiPromptPanel />
                </>
            )}
        </div>
    );
}
