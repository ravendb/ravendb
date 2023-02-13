import React, { useState } from "react";
import { NodeSet, NodeSetLabel, NodeSetItem, NodeSetListCard } from "./NodeSet";
import { Checkbox } from "./Checkbox";
import { CheckboxTriple } from "components/common/CheckboxTriple";
import { Label } from "reactstrap";

interface MultipleDatabaseLocationSelectorProps {
    locations: databaseLocationSpecifier[];
    selectedLocations: databaseLocationSpecifier[];
    setSelectedLocations: React.Dispatch<React.SetStateAction<databaseLocationSpecifier[]>>;
}

export function MultipleDatabaseLocationSelector(props: MultipleDatabaseLocationSelectorProps) {
    const { locations, selectedLocations, setSelectedLocations } = props;

    const [uniqId] = useState(() => _.uniqueId("location-selector-"));

    const isAllNodesSelected: boolean = locations.length === selectedLocations.length;

    const isShardSelected = (location: databaseLocationSpecifier): boolean => selectedLocations.includes(location);

    const isNodeSelected = (nodeTag: string): boolean => {
        return (
            selectedLocations.filter((x) => x.nodeTag === nodeTag).length ===
            locations.filter((x) => x.nodeTag === nodeTag).length
        );
    };

    const toggleAllNodes = () => {
        if (isAllNodesSelected) {
            setSelectedLocations([]);
        } else {
            setSelectedLocations(locations);
        }
    };

    const toggleShard = (location: databaseLocationSpecifier) => {
        if (isShardSelected(location)) {
            setSelectedLocations(selectedLocations.filter((x) => x !== location));
        } else {
            setSelectedLocations([...selectedLocations, location]);
        }
    };

    const toggleNode = (nodeTag: string) => {
        setSelectedLocations((prev) => {
            const filtered = prev.filter((x) => x.nodeTag !== nodeTag);

            if (isNodeSelected(nodeTag)) {
                return filtered;
            } else {
                return [...filtered, ...locations.filter((x) => x.nodeTag === nodeTag)];
            }
        });
    };

    const nodesSelectionState = (): checkbox => {
        if (isAllNodesSelected) {
            return "checked";
        }
        if (selectedLocations.length === 0) {
            return "unchecked";
        }

        return "some_checked";
    };

    const uniqueNodeTags = [...new Set(locations.map((x) => x.nodeTag))];

    return (
        <div>
            <CheckboxTriple onChanged={toggleAllNodes} state={nodesSelectionState()} title="Select all or none" />

            {uniqueNodeTags.map((nodeTag, idx) => {
                const nodeId = uniqId + idx;
                return (
                    <div key={nodeId}>
                        <NodeSet color="shard" className="m-1">
                            <NodeSetListCard>
                                <NodeSetItem icon="node" color="node">
                                    <Label>
                                        {nodeTag}
                                        <Checkbox
                                            toggleSelection={() => toggleNode(nodeTag)}
                                            selected={isNodeSelected(nodeTag)}
                                        />
                                    </Label>
                                </NodeSetItem>
                            </NodeSetListCard>

                            {locations
                                .filter((x) => x.nodeTag === nodeTag)
                                .map((location) => {
                                    if (location.shardNumber == null) {
                                        return null;
                                    }

                                    return (
                                        <NodeSetLabel
                                            key={nodeId + "-shard-" + location.shardNumber}
                                            color="shard"
                                            icon="shard"
                                        >
                                            <Label>
                                                {location.shardNumber}
                                                <Checkbox
                                                    selected={isShardSelected(location)}
                                                    toggleSelection={() => toggleShard(location)}
                                                    color="shard"
                                                />
                                            </Label>
                                        </NodeSetLabel>
                                    );
                                })}
                        </NodeSet>
                    </div>
                );
            })}
        </div>
    );
}
