import "./Chatbot.scss";
import { useAppSelector } from "components/store";
import { chatbotSelectors } from "./store/chatbotSlice";
import useResizableWidth from "components/hooks/useResizableWidth";
import ColumnResize from "components/common/ColumnResize";
import classNames from "classnames";
import ChatbotBody from "./partials/ChatbotBody";
import ChatbotFooter from "./partials/ChatbotFooter";
import ChatbotHeader from "./partials/ChatbotHeader";

export default function Chatbot() {
    const isOpen = useAppSelector(chatbotSelectors.isOpen);
    const isPinned = useAppSelector(chatbotSelectors.isPinned);
    const absoluteNotificationsWidth = useAppSelector(chatbotSelectors.absoluteNotificationsWidth);

    const resizable = useResizableWidth({
        initialWidth: 400,
        minWidth: 400,
        maxWidth: 600,
    });

    if (!isOpen) {
        return null;
    }

    const positionStyle: React.CSSProperties = isPinned
        ? { position: "relative" }
        : { position: "absolute", right: 10 + absoluteNotificationsWidth, top: 10, bottom: 10 };

    return (
        <div
            className={classNames("chatbot panel-bg-1 border-secondary vstack", {
                "h-100 border-left": isPinned,
                "border rounded-2": !isPinned,
            })}
            style={{
                ...positionStyle,
                width: `${resizable.width}px`,
                borderLeft: `1px solid ${resizable.isDragging ? "#ccc" : "#4c4c63"}`,
            }}
        >
            <ColumnResize handleMouseDown={resizable.handleMouseDown} />
            <ChatbotHeader />
            <ChatbotBody />
            <ChatbotFooter />
        </div>
    );
}
