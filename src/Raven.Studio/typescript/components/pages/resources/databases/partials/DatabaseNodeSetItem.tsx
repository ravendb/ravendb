import { NodeInfo } from "components/models/databases";
import { NodeSetItem } from "components/common/NodeSet";
import React from "react";
import assertUnreachable from "components/utils/assertUnreachable";
import IconName from "typings/server/icons";
import { TextColor } from "components/models/common";

export function DatabaseNodeSetItem(props: { node: NodeInfo; isOfflineOrDisabled?: boolean }) {
    const { node, isOfflineOrDisabled } = props;

    return (
        <NodeSetItem
            key={node.tag}
            icon={iconForNodeType(node.type)}
            color={colorForNodeType(node.type, isOfflineOrDisabled)}
            title={node.type}
        >
            {node.tag}
        </NodeSetItem>
    );
}

function colorForNodeType(type: databaseGroupNodeType, isOfflineOrDisabled?: boolean): TextColor {
    if (isOfflineOrDisabled) {
        return "muted";
    }

    switch (type) {
        case "Member":
            return "node";
        case "Rehab":
            return "danger";
        case "Promotable":
            return "warning";
        default:
            assertUnreachable(type);
    }
}

function iconForNodeType(type: databaseGroupNodeType): IconName {
    switch (type) {
        case "Member":
            return "dbgroup-member";
        case "Rehab":
            return "dbgroup-rehab";
        case "Promotable":
            return "dbgroup-promotable";
        default:
            assertUnreachable(type);
    }
}
