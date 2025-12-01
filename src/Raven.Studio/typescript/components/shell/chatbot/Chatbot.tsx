import "./Chatbot.scss";
import useResizableWidth from "components/hooks/useResizableWidth";
import ColumnResize from "components/common/ColumnResize";
import ChatbotBody from "./partials/ChatbotBody";
import ChatbotFooter from "./partials/ChatbotFooter";
import ChatbotHeader from "./partials/ChatbotHeader";
import { useEffect } from "react";
import studioSettings from "common/settings/studioSettings";
import { useAppDispatch } from "components/store";
import { chatbotActions } from "components/shell/chatbot/store/chatbotSlice";

export default function Chatbot() {
    const dispatch = useAppDispatch();

    const resizable = useResizableWidth({
        initialWidth: 400,
        minWidth: 400,
        maxWidth: 600,
        isSkipChatbot: true,
    });

    useEffect(() => {
        setWidthProperty(resizable.width);
    }, [resizable.width]);

    useEffect(() => {
        (async () => {
            const globalSettings = await studioSettings.default.globalSettings();
            const isAlwaysAllowEndpointCalls = globalSettings.isChatbotAlwaysAllowEndpointCalls.getValue();
            dispatch(chatbotActions.isAlwaysAllowEndpointCallsSet(isAlwaysAllowEndpointCalls));
        })();
    }, []);

    return (
        <div
            id="chatbot"
            style={{
                borderLeft: `1px solid ${resizable.isDragging ? "#ccc" : "#4c4c63"}`,
                width: resizable.width,
            }}
        >
            <ColumnResize handleMouseDown={resizable.handleMouseDown} />
            <ChatbotHeader />
            <ChatbotBody />
            <ChatbotFooter />
        </div>
    );
}

function setWidthProperty(width: number) {
    document.documentElement.style.setProperty("--chatbot-width", width + "px");
}
