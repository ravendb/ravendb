import React, { useState } from "react";
import { Card, Label } from "reactstrap";
import { NodeSet, NodeSetItem, NodeSetLabel, NodeSetList } from "./NodeSet";
import { Icon } from "./Icon";
import { Radio } from "./Checkbox";

interface SingleDatabaseLocationSelectorProps {
    locations: databaseLocationSpecifier[];
    selectedLocation: databaseLocationSpecifier;
    setSelectedLocation: (location: databaseLocationSpecifier) => void;
}

export function SingleDatabaseLocationSelector(props: SingleDatabaseLocationSelectorProps) {
    const { locations, selectedLocation, setSelectedLocation } = props;
    const [uniqId] = useState(() => _.uniqueId("single-location-selector-"));

    const uniqueNodeTags = [...new Set(locations.map((x) => x.nodeTag))];
    const isNonSharded = uniqueNodeTags.length === locations.length;

    return (
        <div className="bs5">
            {isNonSharded ? (
                <>
                    <NodeSet>
                        <NodeSetList>
                            {locations.map((location, idx) => {
                                const locationId = uniqId + idx;
                                return (
                                    <>
                                        <NodeSetItem key={locationId}>
                                            <Label htmlFor={locationId}>
                                                <Icon icon="node" color="node" title={"Node " + location.nodeTag} />
                                                {location.nodeTag}
                                                <div className="d-flex justify-content-center">
                                                    <Radio
                                                        id={locationId}
                                                        selected={selectedLocation === location}
                                                        toggleSelection={() => setSelectedLocation(location)}
                                                    />
                                                </div>
                                            </Label>
                                        </NodeSetItem>
                                    </>
                                );
                            })}
                        </NodeSetList>
                    </NodeSet>
                </>
            ) : (
                <>
                    {uniqueNodeTags.map((nodeTag, idx) => {
                        const nodeId = uniqId + idx;

                        return (
                            <div key={nodeId}>
                                <NodeSet className="my-1">
                                    <NodeSetLabel color="node" icon="node" title={"Node " + nodeTag}>
                                        {nodeTag}
                                    </NodeSetLabel>
                                    <div className="node-set-separator" />
                                    <NodeSetList>
                                        {locations
                                            .filter((x) => x.nodeTag === nodeTag)
                                            .map((location) => {
                                                const shardId = nodeId + "-shard-" + location.shardNumber;
                                                return (
                                                    <NodeSetItem color="shard">
                                                        <Label
                                                            key={shardId}
                                                            htmlFor={shardId}
                                                            title={"Shard " + location.shardNumber}
                                                        >
                                                            <Icon icon="shard" color="shard" /> {location.shardNumber}
                                                            <div className="d-flex justify-content-center">
                                                                <Radio
                                                                    id={shardId}
                                                                    selected={selectedLocation === location}
                                                                    toggleSelection={() =>
                                                                        setSelectedLocation(location)
                                                                    }
                                                                    color="shard"
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
        </div>
    );
}
