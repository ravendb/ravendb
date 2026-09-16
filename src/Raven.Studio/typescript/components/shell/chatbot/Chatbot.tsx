import "./Chatbot.scss";
import useResizableWidth from "components/hooks/useResizableWidth";
import ColumnResize from "components/common/ColumnResize";
import ChatbotBody from "./partials/ChatbotBody";
import ChatbotFooter from "./partials/ChatbotFooter";
import ChatbotHeader from "./partials/ChatbotHeader";
import { useEffect } from "react";
import studioSettings from "common/settings/studioSettings";
import { useAppDispatch, useAppSelector } from "components/store";
import { chatbotActions, chatbotSelectors } from "components/shell/chatbot/store/chatbotSlice";
import classNames from "classnames";

export default function Chatbot() {
    const dispatch = useAppDispatch();
    const isNewContextOpen = useAppSelector(chatbotSelectors.isNewContextOpen);

    const resizable = useResizableWidth({
        initialWidth: 400,
        minWidth: 400,
        maxWidth: 600,
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
                width: resizable.width,
            }}
            className={classNames({ "is-dragging": resizable.isDragging })}
        >
            <ColumnResize
                handleMouseDown={(e) => {
                    resizable.handleMouseDown(e);

                    if (isNewContextOpen) {
                        dispatch(chatbotActions.isNewContextOpenSet(false));
                    }
                }}
            />
            <ChatbotHeader />
            <ChatbotBody />
            <ChatbotFooter />
        </div>
    );
}

function setWidthProperty(width: number) {
    document.documentElement.style.setProperty("--chatbot-width", width + "px");
}
