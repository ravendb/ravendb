import { useAppSelector } from "components/store";
import { chatbotSelectors } from "../../store/chatbotSlice";
import { aiAssistantSelectors } from "components/common/shell/aiAssistantSlice";
import { Icon } from "components/common/Icon";

export default function ChatbotAskAiWelcome() {
    const consentStatus = useAppSelector(aiAssistantSelectors.consentStatus);
    const messagesCount = useAppSelector(chatbotSelectors.messagesCount);

    const isConsentSuccess = consentStatus.data === "Success";

    if (messagesCount > 0 || !isConsentSuccess) {
        return null;
    }

    return (
        <div className="pt-2 px-2 text-center">
            <Icon icon="ask-ai" margin="m-0" size="lg" />
            <h3 className="mt-1">Ask anything about RavenDB or your Database</h3>
            <div>AI responses may be inaccurate.</div>
        </div>
    );
}
