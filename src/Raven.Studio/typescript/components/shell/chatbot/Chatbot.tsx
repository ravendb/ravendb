import "./Chatbot.scss";
import useResizableWidth from "components/hooks/useResizableWidth";
import ColumnResize from "components/common/ColumnResize";
import ChatbotBody from "./partials/ChatbotBody";
import ChatbotFooter from "./partials/ChatbotFooter";
import ChatbotHeader from "./partials/ChatbotHeader";
import { useEffect } from "react";

export default function Chatbot() {
    const resizable = useResizableWidth({
        initialWidth: 400,
        minWidth: 400,
        maxWidth: 600,
        isSkipChatbot: true,
    });

    useEffect(() => {
        setWidthProperty(resizable.width);
    }, [resizable.width]);

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
