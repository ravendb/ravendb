import useBoolean from "components/hooks/useBoolean";
import { useState, useCallback, useEffect } from "react";
import useClassNamesObserver from "./useClassNamesObserver";

interface useResizableWidthProps {
    initialWidth: number;
    minWidth: number;
    maxWidth: number;
    isSkipChatbot?: boolean; // used only in Chatbot component
}

export default function useResizableWidth({ initialWidth, minWidth, maxWidth, isSkipChatbot }: useResizableWidthProps) {
    const [width, setWidth] = useState(initialWidth);
    const { value: isDragging, setValue: setIsDragging } = useBoolean(false);

    const layoutClassObserver = useClassNamesObserver(document.querySelector(".layout-container"));

    useEffect(() => {
        setWidth(initialWidth);
    }, [initialWidth]);

    const handleMouseDown = useCallback((e: React.MouseEvent) => {
        setIsDragging(true);
        e.preventDefault();
    }, []);

    const handleMouseMove = useCallback(
        (e: MouseEvent) => {
            if (isDragging) {
                let newWidth = window.innerWidth - e.clientX;

                const isChatbotVisibleAndPinned = layoutClassObserver.hasClassNames(["pin-chatbot", "show-chatbot"]);
                const isNotificationsVisibleAndPinned = layoutClassObserver.hasClassNames([
                    "pin-notifications",
                    "show-notifications",
                ]);

                if (isChatbotVisibleAndPinned && !isSkipChatbot) {
                    const chatbotWidthInPx = parseInt(
                        document.getElementById("chatbot").style.getPropertyValue("width")
                    );
                    newWidth -= chatbotWidthInPx;
                }

                if (isNotificationsVisibleAndPinned) {
                    newWidth -= 440;
                }

                const fixedWidth = Math.max(minWidth, Math.min(maxWidth, newWidth));

                setWidth(fixedWidth);
            }
        },
        [isDragging]
    );

    const handleMouseUp = useCallback(() => {
        setIsDragging(false);
    }, []);

    useEffect(() => {
        if (isDragging) {
            document.addEventListener("mousemove", handleMouseMove);
            document.addEventListener("mouseup", handleMouseUp);
            return () => {
                document.removeEventListener("mousemove", handleMouseMove);
                document.removeEventListener("mouseup", handleMouseUp);
            };
        }
    }, [isDragging, handleMouseMove, handleMouseUp]);

    return {
        width,
        isDragging,
        handleMouseDown,
    };
}
