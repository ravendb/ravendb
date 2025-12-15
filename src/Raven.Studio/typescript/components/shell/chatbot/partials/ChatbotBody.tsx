import { useAppSelector } from "components/store";
import { chatbotSelectors } from "../store/chatbotSlice";
import ChatbotPanelAskAi from "./ChatbotPanelAskAi";
import ChatbotPanelResources from "./resources/ChatbotPanelResources";
import { Activity } from "react";

export default function ChatbotBody() {
    const chatbotTab = useAppSelector(chatbotSelectors.chatbotTab);

    return (
        <div className="vstack flex-grow-1" style={{ minHeight: 0 }}>
            <Activity mode={chatbotTab === "askAi" ? "visible" : "hidden"}>
                <ChatbotPanelAskAi />
            </Activity>
            <Activity mode={chatbotTab === "resources" ? "visible" : "hidden"}>
                <ChatbotPanelResources />
            </Activity>
        </div>
    );
}
