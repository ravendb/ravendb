import { MouseEvent, useCallback, useEffect, useRef, useState } from "react";
import { LazyTableSelection } from "components/common/virtualTable/hooks/useLazyTableSelection";

export function useSelectionPreview<T>(selection: LazyTableSelection<T> | undefined) {
    const [previewTargetRowIndex, setPreviewTargetRowIndex] = useState<number>(null);
    const hoveredRowIndexRef = useRef<number>(null);
    const isShiftPressedRef = useRef(false);

    const anchorRowIndex = selection?.anchorRowIndex ?? null;
    const anchorRowIndexRef = useRef(anchorRowIndex);
    anchorRowIndexRef.current = anchorRowIndex;

    const hasSelection = selection != null;

    const updatePreviewTarget = useCallback(() => {
        const nextTarget =
            isShiftPressedRef.current && anchorRowIndexRef.current !== null ? hoveredRowIndexRef.current : null;

        setPreviewTargetRowIndex(nextTarget);
    }, []);

    // Tracks the shift key on the document, so the preview follows it without moving the mouse
    useEffect(() => {
        if (!hasSelection) {
            return;
        }

        const handleKey = (e: KeyboardEvent) => {
            isShiftPressedRef.current = e.shiftKey;
            updatePreviewTarget();
        };
        const handleBlur = () => {
            isShiftPressedRef.current = false;
            updatePreviewTarget();
        };

        document.addEventListener("keydown", handleKey);
        document.addEventListener("keyup", handleKey);
        window.addEventListener("blur", handleBlur);

        return () => {
            document.removeEventListener("keydown", handleKey);
            document.removeEventListener("keyup", handleKey);
            window.removeEventListener("blur", handleBlur);
        };
    }, [hasSelection, updatePreviewTarget]);

    const onMouseMove = (e: MouseEvent) => {
        const row = (e.target as Element).closest<HTMLElement>("tr[data-row-index]");

        hoveredRowIndexRef.current = row ? Number(row.dataset.rowIndex) : null;
        isShiftPressedRef.current = e.shiftKey;
        updatePreviewTarget();
    };

    const onMouseLeave = () => {
        hoveredRowIndexRef.current = null;
        updatePreviewTarget();
    };

    const isPreviewVisible =
        anchorRowIndex !== null && previewTargetRowIndex !== null && selection.canSelectRangeTo(previewTargetRowIndex);

    const isPreviewed = (rowIndex: number) =>
        isPreviewVisible &&
        rowIndex >= Math.min(anchorRowIndex, previewTargetRowIndex) &&
        rowIndex <= Math.max(anchorRowIndex, previewTargetRowIndex);

    return {
        isPreviewed,
        onMouseMove: hasSelection ? onMouseMove : undefined,
        onMouseLeave: hasSelection ? onMouseLeave : undefined,
    };
}
