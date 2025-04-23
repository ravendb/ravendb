import { CSSProperties } from "react";
import { CSS } from "@dnd-kit/utilities";
import { useSortable } from "@dnd-kit/sortable";

export function useSortableStyles({ transform, isDragging, transition }: ReturnType<typeof useSortable>) {
    const style: CSSProperties = {
        transform: CSS.Transform.toString(transform),
        cursor: isDragging ? "grabbing" : "grab",
        opacity: isDragging ? 0.5 : 1,
        transition,
    };

    return style;
}
