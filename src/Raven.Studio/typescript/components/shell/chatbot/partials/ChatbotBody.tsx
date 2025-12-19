import { useAppSelector } from "components/store";
import { chatbotSelectors } from "../store/chatbotSlice";
import ChatbotPanelAskAi from "./ChatbotPanelAskAi";
import ChatbotPanelResources from "./resources/ChatbotPanelResources";
import ChatbotPanelWhatsNew from "./ChatbotPanelWhatsNew";
import ChatbotPanelNews from "./ChatbotPanelNews";

export default function ChatbotBody() {
    const chatbotTab = useAppSelector(chatbotSelectors.chatbotTab);

    return (
        <div className="vstack flex-grow-1" style={{ minHeight: 0 }}>
            {chatbotTab === "Ask AI" && <ChatbotPanelAskAi />}
            {chatbotTab === "What's new" && <ChatbotPanelWhatsNew />}
            {chatbotTab === "News" && <ChatbotPanelNews />}
            {chatbotTab === "Resources" && <ChatbotPanelResources />}
        </div>
    );
}
