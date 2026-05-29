import { useEffect, useState } from "react";

type UseResizableHeightOptions = {
    initialHeight: number;
    maxHeight: number;
    minHeight: number;
};

function clampHeight(height: number, minHeight: number, maxHeight: number) {
    return Math.min(Math.max(height, minHeight), maxHeight);
}

export function useResizableHeight({ initialHeight, maxHeight, minHeight }: UseResizableHeightOptions) {
    const [height, setHeightState] = useState(() => clampHeight(initialHeight, minHeight, maxHeight));
    const [dragStart, setDragStart] = useState<{ height: number; y: number } | null>(null);

    useEffect(() => {
        const currentDragStart = dragStart;

        if (!currentDragStart) {
            return;
        }
        const startHeight = currentDragStart.height;
        const startY = currentDragStart.y;

        function handleMouseMove(event: MouseEvent) {
            setHeightState(clampHeight(startHeight + event.clientY - startY, minHeight, maxHeight));
        }

        function handleMouseUp() {
            setDragStart(null);
        }

        window.addEventListener("mousemove", handleMouseMove);
        window.addEventListener("mouseup", handleMouseUp);
        document.body.style.userSelect = "none";

        return () => {
            window.removeEventListener("mousemove", handleMouseMove);
            window.removeEventListener("mouseup", handleMouseUp);
            document.body.style.userSelect = "";
        };
    }, [dragStart, maxHeight, minHeight]);

    function setHeight(nextHeight: number) {
        setHeightState(clampHeight(nextHeight, minHeight, maxHeight));
    }

    function handleMouseDown(event: React.MouseEvent) {
        event.preventDefault();
        setDragStart({ height, y: event.clientY });
    }

    return {
        handleMouseDown,
        height,
        isDragging: dragStart !== null,
        setHeight,
    };
}
