import { NodeInfo } from "components/models/databases";
import { NodeSetItem } from "components/common/NodeSet";
import React from "react";
import assertUnreachable from "components/utils/assertUnreachable";

export function DatabaseNodeSetItem(props: { node: NodeInfo }) {
    const { node } = props;
    return (
        <NodeSetItem
            key={node.tag}
            icon={iconForNodeType(node.type)}
            color={colorForNodeType(node.type)}
            title={node.type}
        >
            {node.tag}
        </NodeSetItem>
    );
}

function colorForNodeType(type: databaseGroupNodeType) {
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

function iconForNodeType(type: databaseGroupNodeType) {
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
