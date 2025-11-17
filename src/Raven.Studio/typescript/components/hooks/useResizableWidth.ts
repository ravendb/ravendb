import useBoolean from "components/hooks/useBoolean";
import { useState, useCallback, useEffect } from "react";
import useClassNamesObserver from "./useClassNamesObserver";

interface useResizableWidthProps {
    initialWidth: number;
    minWidth: number;
    maxWidth: number;
}

export default function useResizableWidth({ initialWidth, minWidth, maxWidth }: useResizableWidthProps) {
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
                const isNotificationsVisibleAndPinned = layoutClassObserver.hasClassNames([
                    "pin-notifications",
                    "show-notifications",
                ]);
                const rightOffset = isNotificationsVisibleAndPinned ? 440 : 0;

                const newWidth = window.innerWidth - e.clientX - rightOffset;
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
