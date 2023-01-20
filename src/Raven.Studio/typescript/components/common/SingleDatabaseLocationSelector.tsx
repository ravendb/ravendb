import React, { useState } from "react";
import { NodeSet, NodeSetItem, NodeSetLabel, NodeSetListCard } from "./NodeSet";
import { Radio } from "./Radio";

interface SingleDatabaseLocationSelectorProps {
    locations: databaseLocationSpecifier[];
    selectedLocation: databaseLocationSpecifier;
    setSelectedLocation: (location: databaseLocationSpecifier) => void;
}

export function SingleDatabaseLocationSelector(props: SingleDatabaseLocationSelectorProps) {
    const { locations, selectedLocation, setSelectedLocation } = props;

    const [uniqId] = useState(() => _.uniqueId("single-location-selector-"));

    const toggleSelection = (location: databaseLocationSpecifier) => {
        setSelectedLocation(location);
    };

    return (
        <div>
            {locations.map((l, idx) => {
                const selected = selectedLocation === l;
                const locationId = uniqId + idx;
                return (
                    <div key={locationId}>
                        <NodeSet color="shard" className="m-1">
                            <NodeSetLabel color="shard" icon="shard">
                                Num
                            </NodeSetLabel>
                            <NodeSetListCard>
                                <NodeSetItem icon="node" color="node">
                                    Tag<Radio toggleSelection={null}></Radio>
                                </NodeSetItem>
                            </NodeSetListCard>
                        </NodeSet>
                    </div>
                );
            })}
        </div>
    );
}
