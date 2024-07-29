import { useState } from "react";
import { NodeInfo } from "components/models/databases";
import { useAppSelector } from "components/store";
import { clusterSelectors } from "components/common/shell/clusterSlice";
import { useSortableModeCounter } from "./useSortableModeCounter";
import { accessManagerSelectors } from "components/common/shell/accessManagerSliceSelectors";

export function useGroup(nodes: NodeInfo[], initialFixOrder: boolean) {
    const [fixOrder, setFixOrder] = useState(initialFixOrder);
    const [newOrder, setNewOrder] = useState<NodeInfo[]>([]);
    const [sortableMode, setSortableMode] = useState(false);
    const clusterNodeTags = useAppSelector(clusterSelectors.allNodeTags);
    const { setCounter: setSortableModeCounter } = useSortableModeCounter();

    const isOperatorOrAbove = useAppSelector(accessManagerSelectors.isOperatorOrAbove);
    const canSort = nodes.length === 1 || !isOperatorOrAbove;

    const enableReorder = () => {
        setNewOrder(nodes.slice());
        setSortableMode(true);
        setSortableModeCounter((counter) => counter + 1);
    };
    const exitReorder = () => {
        setSortableMode(false);
        setSortableModeCounter((counter) => counter - 1);
    };

    const existingTags = nodes ? nodes.map((x) => x.tag) : [];
    const addNodeEnabled = isOperatorOrAbove && clusterNodeTags.some((x) => !existingTags.includes(x));

    return {
        fixOrder,
        setFixOrder,
        newOrder,
        setNewOrder,
        canSort,
        sortableMode,
        enableReorder,
        exitReorder,
        addNodeEnabled,
    };
}
