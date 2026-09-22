import { RefObject, useEffect, useState } from "react";
import { RowRange } from "../utils/lazyTableUtils";

interface UseSelectionPreviewRangeProps {
    // absolute index of the row toggled last, a shift-click selects the range between it and the hovered row
    anchorRowIndex: number | null;
    // scrollable table container, rows are recognized by the data-row-index attribute
    containerRef: RefObject<HTMLElement>;
}

// rows (end exclusive) that would be selected by a shift-click on the hovered row, null when shift is not held
export function useSelectionPreviewRange({ anchorRowIndex, containerRef }: UseSelectionPreviewRangeProps) {
    const [hoveredRowIndex, setHoveredRowIndex] = useState<number>(null);
    const [isShiftPressed, setIsShiftPressed] = useState(false);

    useEffect(() => {
        const element = containerRef.current;
        if (anchorRowIndex === null || !element) {
            return;
        }

        const handleKey = (e: KeyboardEvent) => setIsShiftPressed(e.shiftKey);
        const handleBlur = () => setIsShiftPressed(false);
        const handleMouseMove = (e: MouseEvent) => {
            setIsShiftPressed(e.shiftKey);
            setHoveredRowIndex(getRowIndex(e.target));
        };
        const handleMouseLeave = () => setHoveredRowIndex(null);

        document.addEventListener("keydown", handleKey);
        document.addEventListener("keyup", handleKey);
        window.addEventListener("blur", handleBlur);
        element.addEventListener("mousemove", handleMouseMove);
        element.addEventListener("mouseleave", handleMouseLeave);

        return () => {
            document.removeEventListener("keydown", handleKey);
            document.removeEventListener("keyup", handleKey);
            window.removeEventListener("blur", handleBlur);
            element.removeEventListener("mousemove", handleMouseMove);
            element.removeEventListener("mouseleave", handleMouseLeave);
            setHoveredRowIndex(null);
        };
    }, [anchorRowIndex, containerRef]);

    if (anchorRowIndex === null || hoveredRowIndex === null || !isShiftPressed) {
        return null;
    }

    const range: RowRange = {
        start: Math.min(anchorRowIndex, hoveredRowIndex),
        end: Math.max(anchorRowIndex, hoveredRowIndex) + 1,
    };

    return range;
}

function getRowIndex(target: EventTarget): number | null {
    const row = target instanceof Element ? target.closest<HTMLElement>("tr[data-row-index]") : null;
    return row ? Number(row.dataset.rowIndex) : null;
}
