import { ComponentMeta } from "@storybook/react";
import { NodeSet, NodeSetLabel, NodeSetItem } from "./NodeSet";
import React from "react";
import { withBootstrap5, withStorybookContexts } from "test/storybookTestUtils";
import { HrHeader } from "./HrHeader";
import { Checkbox } from "./Checkbox";
import { Card } from "reactstrap";
import { CheckboxTriple } from "./CheckboxTriple";

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
                <NodeSetItem icon="node" color="node">
                    A
                </NodeSetItem>
                <NodeSetItem icon="node" color="node">
                    B
                </NodeSetItem>
            </NodeSet>
            <HrHeader>Orchestrators</HrHeader>
            <NodeSet color="warning" className="m-1">
                <NodeSetLabel color="warning" icon="orchestrator">
                    Orchestrators
                </NodeSetLabel>
                <NodeSetItem icon="zombie" color="danger">
                    A
                </NodeSetItem>
                <NodeSetItem icon="node" color="node">
                    B
                </NodeSetItem>
            </NodeSet>
            <HrHeader>Shards</HrHeader>

            <NodeSet color="shard" className="m-1">
                <div className="p-1">
                    <NodeSetLabel color="shard" icon="shard">
                        #1
                        {/* <CheckboxTriple
                            onChanged={toggleSelectAll}
                            state={indexesSelectionState()}
                            title="Select all or none"
                        /> */}
                        <Checkbox color="shard" toggleSelection={null} className="me-1 checkbox-lg" />
                    </NodeSetLabel>
                </div>
                <Card className="p-1 flex-row">
                    <NodeSetItem icon="node" color="node">
                        A
                        <Checkbox color="node" toggleSelection={null} />
                    </NodeSetItem>
                    <NodeSetItem icon="node" color="node">
                        B
                        <Checkbox color="node" toggleSelection={null} />
                    </NodeSetItem>
                </Card>
            </NodeSet>
            <br />
            <div className="d-flex mt-3">
                <Checkbox color="shard" toggleSelection={null} />
                <NodeSet color="shard" className="m-1">
                    <NodeSetLabel color="shard" icon="shard">
                        #2
                    </NodeSetLabel>
                    <NodeSetItem icon="node" color="node">
                        A
                    </NodeSetItem>
                    <NodeSetItem icon="node" color="node">
                        B
                    </NodeSetItem>
                    <NodeSetItem icon="node" color="node">
                        DEV
                    </NodeSetItem>
                </NodeSet>
            </div>
            <HrHeader>Nodes</HrHeader>
            <NodeSet color="node" className="m-1">
                <NodeSetLabel color="node" icon="node">
                    PROD
                </NodeSetLabel>
                <NodeSetItem icon="shard" color="shard">
                    #1
                </NodeSetItem>
                <NodeSetItem icon="shard" color="shard">
                    #2
                </NodeSetItem>
            </NodeSet>
            <NodeSet color="node" className="m-1">
                <NodeSetLabel color="node" icon="node">
                    DEV
                </NodeSetLabel>
                <NodeSetItem icon="shard" color="shard">
                    #2
                </NodeSetItem>
                <NodeSetItem icon="shard" color="shard">
                    #3
                </NodeSetItem>
            </NodeSet>
        </div>
    );
}
