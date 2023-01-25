import { useState } from "react";
import { NodeInfo } from "components/models/databases";
import { useAccessManager } from "hooks/useAccessManager";
import { useClusterTopologyManager } from "hooks/useClusterTopologyManager";

export function useGroup(nodes: NodeInfo[]) {
    const [fixOrder, setFixOrder] = useState(false); //TODO: init with data from RavenDB-19876
    const [newOrder, setNewOrder] = useState<NodeInfo[]>([]);
    const [sortableMode, setSortableMode] = useState(false);
    const { nodeTags: clusterNodeTags } = useClusterTopologyManager();

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
