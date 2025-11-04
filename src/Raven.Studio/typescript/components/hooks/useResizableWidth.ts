import useBoolean from "components/hooks/useBoolean";
import { useState, useCallback, useEffect } from "react";

interface useResizableWidthProps {
    initialWidth: number;
    minWidth: number;
    maxWidth: number;
    rightOffset?: number;
}

export default function useResizableWidth({
    initialWidth,
    minWidth,
    maxWidth,
    rightOffset = 0,
}: useResizableWidthProps) {
    const [width, setWidth] = useState(initialWidth);
    const { value: isDragging, setValue: setIsDragging } = useBoolean(false);

    const handleMouseDown = useCallback((e: React.MouseEvent) => {
        setIsDragging(true);
        e.preventDefault();
    }, []);

    const handleMouseMove = useCallback(
        (e: MouseEvent) => {
            if (isDragging) {
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
