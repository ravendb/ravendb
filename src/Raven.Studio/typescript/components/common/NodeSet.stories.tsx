import { ComponentMeta } from "@storybook/react";
import { NodeSet, NodeSetLabel, NodeSetItem, NodeSetList } from "./NodeSet";
import React from "react";
import { withBootstrap5, withStorybookContexts } from "test/storybookTestUtils";
import { HrHeader } from "./HrHeader";

export default {
    title: "Bits/NodeSet",
    component: NodeSet,
    decorators: [withStorybookContexts, withBootstrap5],
} as ComponentMeta<typeof NodeSet>;

export function NodeSets() {
    return (
        <div>
            <HrHeader>Basic</HrHeader>
            <NodeSet className="m-1">
                <NodeSetLabel color="primary" icon="database">
                    Nordwind
                </NodeSetLabel>
                <NodeSetList>
                    <NodeSetItem icon="node" color="node">
                        A
                    </NodeSetItem>
                    <NodeSetItem icon="node" color="node">
                        B
                    </NodeSetItem>
                </NodeSetList>
            </NodeSet>
            <HrHeader>Orchestrators</HrHeader>
            <NodeSet color="warning" className="m-1">
                <NodeSetLabel color="warning" icon="orchestrator">
                    Orchestrators
                </NodeSetLabel>
                <NodeSetList>
                    <NodeSetItem icon="zombie" color="danger">
                        A
                    </NodeSetItem>
                    <NodeSetItem icon="node" color="node">
                        B
                    </NodeSetItem>
                </NodeSetList>
            </NodeSet>
            <HrHeader>Shards</HrHeader>
            <NodeSet color="shard" className="m-1">
                <NodeSetLabel color="shard" icon="shard">
                    #1
                </NodeSetLabel>
                <NodeSetList>
                    <NodeSetItem icon="node" color="node">
                        A
                    </NodeSetItem>
                    <NodeSetItem icon="node" color="node">
                        B
                    </NodeSetItem>
                </NodeSetList>
            </NodeSet>
            <NodeSet color="shard" className="m-1">
                <NodeSetLabel color="shard" icon="shard">
                    #2
                </NodeSetLabel>
                <NodeSetList>
                    <NodeSetItem icon="node" color="node">
                        A
                    </NodeSetItem>
                    <NodeSetItem icon="node" color="node">
                        B
                    </NodeSetItem>
                    <NodeSetItem icon="node" color="node">
                        DEV
                    </NodeSetItem>
                </NodeSetList>
            </NodeSet>
            <HrHeader>Nodes</HrHeader>
            <NodeSet color="node" className="m-1">
                <NodeSetLabel color="node" icon="node">
                    PROD
                </NodeSetLabel>
                <NodeSetList>
                    <NodeSetItem icon="shard" color="shard">
                        #1
                    </NodeSetItem>
                    <NodeSetItem icon="shard" color="shard">
                        #2
                    </NodeSetItem>
                </NodeSetList>
            </NodeSet>
            <NodeSet color="node" className="m-1">
                <NodeSetLabel color="node" icon="node">
                    DEV
                </NodeSetLabel>
                <NodeSetList>
                    <NodeSetItem icon="shard" color="shard">
                        #2
                    </NodeSetItem>
                    <NodeSetItem icon="shard" color="shard">
                        #3
                    </NodeSetItem>
                </NodeSetList>
            </NodeSet>
        </div>
    );
}
