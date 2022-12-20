import { useDrag, useDrop } from "react-dnd";
import { useCallback } from "react";
import { produce } from "immer";

type NodeAndIndex<T> = { item: T; originalIndex: number };

export function useDraggableItem<T>(
    type: string,
    item: T,
    idExtractor: (item: T) => string,
    findIndex: (node: T) => number,
    setNewOrder: (action: (state: T[]) => T[]) => void
) {
    const originalIndex = findIndex(item);

    const moveItem = useCallback(
        (node: T, atIndex: number) => {
            const cardIdx = findIndex(node);

            setNewOrder((prevOrder) => {
                return produce(prevOrder, (draft) => {
                    draft.splice(cardIdx, 1);
                    draft.splice(atIndex, 0, node as any);
                });
            });
        },
        [findIndex, setNewOrder]
    );

    const [{ isDragging }, drag] = useDrag(
        () => ({
            type,
            item: { item, originalIndex } as NodeAndIndex<T>,
            collect: (monitor) => ({
                isDragging: monitor.isDragging(),
            }),
            end: (item, monitor) => {
                const { item: droppedNode, originalIndex } = item;
                const didDrop = monitor.didDrop();
                if (!didDrop) {
                    moveItem(droppedNode, originalIndex);
                }
            },
        }),
        [item, originalIndex, moveItem]
    );

    const [, drop] = useDrop(
        () => ({
            accept: type,
            hover({ item: draggedNode }: NodeAndIndex<T>) {
                if (idExtractor(draggedNode) !== idExtractor(item)) {
                    const overIndex = findIndex(item);
                    moveItem(draggedNode, overIndex);
                }
            },
        }),
        [findIndex, moveItem]
    );

    return {
        drag,
        drop,
        isDragging,
    };
}
