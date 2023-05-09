import { useState } from "react";
import { NodeInfo } from "components/models/databases";
import { useAccessManager } from "hooks/useAccessManager";
import { useAppSelector } from "components/store";
import { clusterSelectors } from "components/common/shell/clusterSlice";

export function useGroup(nodes: NodeInfo[], initialFixOrder: boolean) {
    const [fixOrder, setFixOrder] = useState(initialFixOrder);
    const [newOrder, setNewOrder] = useState<NodeInfo[]>([]);
    const [sortableMode, setSortableMode] = useState(false);
    const clusterNodeTags = useAppSelector(clusterSelectors.allNodeTags);

    const { isOperatorOrAbove } = useAccessManager();
    const canSort = nodes.length === 1 || !isOperatorOrAbove();

    const enableReorder = () => {
        setNewOrder(nodes.slice());
        setSortableMode(true);
    };
    const exitReorder = () => setSortableMode(false);

    const existingTags = nodes ? nodes.map((x) => x.tag) : [];
    const addNodeEnabled = isOperatorOrAbove() && clusterNodeTags.some((x) => !existingTags.includes(x));

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
