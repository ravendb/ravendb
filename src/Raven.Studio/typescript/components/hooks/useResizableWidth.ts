import useBoolean from "components/hooks/useBoolean";
import { useState, useCallback, useEffect, useRef } from "react";

interface useResizableWidthProps {
    initialWidth: number;
    minWidth: number;
    maxWidth: number;
    placement?: "left" | "right";
}

export default function useResizableWidth({
    initialWidth,
    minWidth,
    maxWidth,
    placement = "left",
}: useResizableWidthProps) {
    const [width, setWidth] = useState(initialWidth);
    const { value: isDragging, setValue: setIsDragging } = useBoolean(false);
    const dragStartRef = useRef({
        x: 0,
        width: initialWidth,
    });

    useEffect(() => {
        setWidth(initialWidth);
    }, [initialWidth]);

    const handleMouseDown = useCallback(
        (e: React.MouseEvent) => {
            dragStartRef.current = {
                x: e.clientX,
                width,
            };
            setIsDragging(true);
            e.preventDefault();
        },
        [setIsDragging, width]
    );

    const handleMouseMove = useCallback(
        (e: MouseEvent) => {
            if (isDragging) {
                const deltaX = e.clientX - dragStartRef.current.x;
                let newWidth = dragStartRef.current.width + deltaX;

                if (placement === "left") {
                    newWidth = dragStartRef.current.width - deltaX;
                }

                const fixedWidth = Math.max(minWidth, Math.min(maxWidth, newWidth));

                setWidth(fixedWidth);
            }
        },
        [isDragging, maxWidth, minWidth, placement]
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
