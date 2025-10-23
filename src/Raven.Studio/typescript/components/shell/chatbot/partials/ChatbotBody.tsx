import { useAppSelector } from "components/store";
import { chatbotSelectors } from "../store/chatbotSlice";
import ChatbotPanelAskAi from "./ChatbotPanelAskAi";
import ChatbotPanelResources from "./resources/ChatbotPanelResources";
import ChatbotPanelWhatsNew from "./ChatbotPanelWhatsNew";
import ChatbotPanelNews from "./ChatbotPanelNews";

export default function ChatbotBody() {
    const chatbotTab = useAppSelector(chatbotSelectors.chatbotTab);

    return (
        <div className="vstack flex-grow-1">
            {chatbotTab === "askAi" && <ChatbotPanelAskAi />}
            {chatbotTab === "whatsNew" && <ChatbotPanelWhatsNew />}
            {chatbotTab === "news" && <ChatbotPanelNews />}
            {chatbotTab === "resources" && <ChatbotPanelResources />}
        </div>
    );
}
