import { useAppSelector } from "components/store";
import { chatbotSelectors } from "../../store/chatbotSlice";
import { aiAssistantSelectors } from "components/common/shell/aiAssistantSlice";
import AsciiLogo from "components/shell/chatbot/partials/askAi/iconAscii/IconAscii";
import KeywordsSlider from "components/shell/chatbot/partials/askAi/ChatbotAskAiKeywordsSlider";

export default function ChatbotAskAiWelcome() {
    const messagesCount = useAppSelector(chatbotSelectors.messagesCount);
    const consentStatus = useAppSelector(aiAssistantSelectors.consentStatus);
    const isConsentSuccess = consentStatus.data === "Success";

    if (messagesCount > 0 || !isConsentSuccess) {
        return null;
    }

    return (
        <div className="p-5 text-center">
            <AsciiLogo />
            <div className="vstack gap-1">
                <h3 className="mt-4 mb-0 fw-semibold">AI Assistant</h3>
                <KeywordsSlider />
            </div>
        </div>
    );
}
