import { MouseEvent, useEffect, useState } from "react";
import { LazyTableSelection } from "components/common/virtualTable/hooks/useLazyTableSelection";

export function useSelectionPreview<T>(selection: LazyTableSelection<T> | undefined) {
    const [hoveredRowIndex, setHoveredRowIndex] = useState<number>(null);
    const [isShiftPressed, setIsShiftPressed] = useState(false);

    // Tracks the shift key on the document, so the preview follows it without moving the mouse
    useEffect(() => {
        const handleKey = (e: KeyboardEvent) => setIsShiftPressed(e.shiftKey);
        const handleBlur = () => setIsShiftPressed(false);

        document.addEventListener("keydown", handleKey);
        document.addEventListener("keyup", handleKey);
        window.addEventListener("blur", handleBlur);

        return () => {
            document.removeEventListener("keydown", handleKey);
            document.removeEventListener("keyup", handleKey);
            window.removeEventListener("blur", handleBlur);
        };
    }, []);

    const onMouseMove = (e: MouseEvent) => {
        const row = (e.target as Element).closest<HTMLElement>("tr[data-row-index]");

        setIsShiftPressed(e.shiftKey);
        setHoveredRowIndex(row ? Number(row.dataset.rowIndex) : null);
    };

    const onMouseLeave = () => setHoveredRowIndex(null);

    const anchorRowIndex = selection?.anchorRowIndex ?? null;
    const isPreviewVisible =
        anchorRowIndex !== null &&
        hoveredRowIndex !== null &&
        isShiftPressed &&
        selection.canSelectRangeTo(hoveredRowIndex);

    const isPreviewed = (rowIndex: number) =>
        isPreviewVisible &&
        rowIndex >= Math.min(anchorRowIndex, hoveredRowIndex) &&
        rowIndex <= Math.max(anchorRowIndex, hoveredRowIndex);

    return { isPreviewed, onMouseMove, onMouseLeave };
}
