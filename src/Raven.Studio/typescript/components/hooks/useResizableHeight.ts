import useBoolean from "components/hooks/useBoolean";
import { useState, useCallback, useEffect, useRef } from "react";

interface UseResizableHeightProps {
    initialHeight: number | string;
    minHeight: number | string;
    maxHeight: number | string;
}

export default function useResizableHeight(props: UseResizableHeightProps) {
    const initialHeight = parseInt(String(props.initialHeight));
    const minHeight = parseInt(String(props.minHeight));
    const maxHeight = parseInt(String(props.maxHeight));

    const [height, setHeight] = useState(initialHeight);
    const { value: isDragging, setValue: setIsDragging } = useBoolean(false);
    const dragStartYRef = useRef<number>(null);
    const dragStartHeightRef = useRef(initialHeight);

    const clampHeight = useCallback(
        (nextHeight: number) => {
            return Math.max(minHeight, Math.min(maxHeight, nextHeight));
        },
        [maxHeight, minHeight]
    );

    const setClampHeight = useCallback(
        (nextHeight: number) => {
            setHeight(clampHeight(nextHeight));
        },
        [clampHeight]
    );

    useEffect(() => {
        const fixedHeight = clampHeight(initialHeight);
        setHeight(fixedHeight);
        dragStartHeightRef.current = fixedHeight;
    }, [clampHeight, initialHeight]);

    const handleMouseDown = useCallback(
        (e: React.MouseEvent) => {
            dragStartYRef.current = e.clientY;
            dragStartHeightRef.current = height;
            setIsDragging(true);
            e.preventDefault();
        },
        [height, setIsDragging]
    );

    const handleMouseMove = useCallback(
        (e: MouseEvent) => {
            if (isDragging && dragStartYRef.current !== null) {
                const deltaY = e.clientY - dragStartYRef.current;
                const newHeight = dragStartHeightRef.current + deltaY;
                setClampHeight(newHeight);
            }
        },
        [isDragging, setClampHeight]
    );

    const handleMouseUp = useCallback(() => {
        dragStartYRef.current = null;
        setIsDragging(false);
    }, [setIsDragging]);

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
        height,
        isDragging,
        handleMouseDown,
        setHeight: setClampHeight,
    };
}
