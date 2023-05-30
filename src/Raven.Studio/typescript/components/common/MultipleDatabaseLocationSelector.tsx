import React, { useState } from "react";
import { NodeSet, NodeSetLabel, NodeSetItem, NodeSetList } from "./NodeSet";
import { Checkbox } from "./Checkbox";
import { Card, Label } from "reactstrap";
import { Icon } from "./Icon";
import classNames from "classnames";

interface MultipleDatabaseLocationSelectorProps {
    locations: databaseLocationSpecifier[];
    selectedLocations: databaseLocationSpecifier[];
    setSelectedLocations: React.Dispatch<React.SetStateAction<databaseLocationSpecifier[]>>;
    className?: string;
}

export function MultipleDatabaseLocationSelector(props: MultipleDatabaseLocationSelectorProps) {
    const { locations, selectedLocations, setSelectedLocations, className } = props;

    const [uniqId] = useState(() => _.uniqueId("location-selector-"));

    const isAllNodesSelected: boolean = locations.length === selectedLocations.length;
    const isSomeNodesSelected: boolean = locations.length > selectedLocations.length && selectedLocations.length > 0;

    const isShardSelected = (location: databaseLocationSpecifier): boolean => selectedLocations.includes(location);

    const isNodeSelected = (nodeTag: string): boolean => {
        return (
            selectedLocations.filter((x) => x.nodeTag === nodeTag).length ===
            locations.filter((x) => x.nodeTag === nodeTag).length
        );
    };

    const isNodeIndeterminate = (nodeTag: string): boolean => {
        return (
            selectedLocations.some((x) => x.nodeTag === nodeTag) &&
            selectedLocations.filter((x) => x.nodeTag === nodeTag).length <
                locations.filter((x) => x.nodeTag === nodeTag).length
        );
    };

    const toggleAllNodes = () => {
        if (isAllNodesSelected || selectedLocations.length !== 0) {
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

            if (isNodeSelected(nodeTag) || isNodeIndeterminate(nodeTag)) {
                return filtered;
            } else {
                return [...filtered, ...locations.filter((x) => x.nodeTag === nodeTag)];
            }
        });
    };

    const uniqueNodeTags = [...new Set(locations.map((x) => x.nodeTag))];

    return (
        <>
            {locations[0].shardNumber == null ? (
                <>
                    <NodeSet className={classNames(className)}>
                        <NodeSetLabel>
                            <Checkbox
                                size="lg"
                                toggleSelection={toggleAllNodes}
                                indeterminate={isSomeNodesSelected}
                                selected={isAllNodesSelected}
                                title="Select all or none"
                            />
                        </NodeSetLabel>
                        <NodeSetList>
                            {uniqueNodeTags.map((nodeTag, idx) => {
                                const nodeId = uniqId + idx;
                                return (
                                    <NodeSetItem key={nodeId}>
                                        <Label htmlFor={nodeId}>
                                            <Icon icon="node" color="node" />
                                            {nodeTag}
                                            <div className="d-flex justify-content-center">
                                                <Checkbox
                                                    id={nodeId}
                                                    toggleSelection={() => toggleNode(nodeTag)}
                                                    selected={isNodeSelected(nodeTag)}
                                                />
                                            </div>
                                        </Label>
                                    </NodeSetItem>
                                );
                            })}
                        </NodeSetList>
                    </NodeSet>
                </>
            ) : (
                <>
                    <div className="d-flex align-items-center gap-1">
                        <Checkbox
                            size="lg"
                            toggleSelection={toggleAllNodes}
                            indeterminate={isSomeNodesSelected}
                            selected={isAllNodesSelected}
                            title="Select all or none"
                        >
                            <span className="small-label">
                                {isAllNodesSelected
                                    ? "Reset selection"
                                    : isSomeNodesSelected
                                    ? "Deselect all"
                                    : "Select all"}
                            </span>
                        </Checkbox>
                    </div>
                    {uniqueNodeTags.map((nodeTag, idx) => {
                        const nodeId = uniqId + idx;
                        return (
                            <div key={nodeId}>
                                <NodeSet color="shard" className={classNames(className, "mt-1")}>
                                    <Card>
                                        <NodeSetLabel>
                                            <Label htmlFor={nodeId}>
                                                <Icon icon="node" color="node" />
                                                {nodeTag}
                                                <div className="d-flex justify-content-center">
                                                    <Checkbox
                                                        id={nodeId}
                                                        indeterminate={isNodeIndeterminate(nodeTag)}
                                                        toggleSelection={() => toggleNode(nodeTag)}
                                                        selected={isNodeSelected(nodeTag)}
                                                    />
                                                </div>
                                            </Label>
                                        </NodeSetLabel>
                                    </Card>
                                    <NodeSetList>
                                        {locations
                                            .filter((x) => x.nodeTag === nodeTag)
                                            .map((location) => {
                                                if (location.shardNumber == null) {
                                                    return null;
                                                }

                                                return (
                                                    <NodeSetItem key={nodeId + "-shard-" + location.shardNumber}>
                                                        <Label htmlFor={nodeId + "-shard-" + location.shardNumber}>
                                                            <Icon icon="shard" color="shard" />
                                                            {location.shardNumber}
                                                            <div className="d-flex justify-content-center">
                                                                <Checkbox
                                                                    selected={isShardSelected(location)}
                                                                    toggleSelection={() => toggleShard(location)}
                                                                    color="shard"
                                                                    id={nodeId + "-shard-" + location.shardNumber}
                                                                />
                                                            </div>
                                                        </Label>
                                                    </NodeSetItem>
                                                );
                                            })}
                                    </NodeSetList>
                                </NodeSet>
                            </div>
                        );
                    })}
                </>
            )}
        </>
    );
}
