import React, { useState } from "react";
import { NodeSet, NodeSetLabel, NodeSetItem, NodeSetListCard } from "./NodeSet";
import { Checkbox } from "./Checkbox";

interface MultipleDatabaseLocationSelectorProps {
    locations: databaseLocationSpecifier[];
    selectedLocations: databaseLocationSpecifier[];
    setSelectedLocations: (locations: databaseLocationSpecifier[]) => void;
}

export function MultipleDatabaseLocationSelector(props: MultipleDatabaseLocationSelectorProps) {
    const { locations, selectedLocations, setSelectedLocations } = props;

    const [uniqId] = useState(() => _.uniqueId("location-selector-"));

    const toggleSelection = (location: databaseLocationSpecifier) => {
        const selected = selectedLocations.includes(location);
        if (selected) {
            setSelectedLocations(selectedLocations.filter((x) => x !== location));
        } else {
            setSelectedLocations([...selectedLocations, location]);
        }
    };

    return (
        <div>
            {locations.map((l, idx) => {
                const selected = selectedLocations.includes(l);
                const locationId = uniqId + idx;
                return (
                    <div key={locationId}>
                        <NodeSet color="shard" className="m-1">
                            <NodeSetLabel color="shard" icon="shard">
                                Num
                                <Checkbox
                                    selected={selected}
                                    toggleSelection={() => toggleSelection(l)}
                                    color="shard"
                                ></Checkbox>
                                {/* TODO: clickable labels */}
                            </NodeSetLabel>
                            <NodeSetListCard>
                                <NodeSetItem icon="node" color="node">
                                    Tag<Checkbox toggleSelection={null}></Checkbox>
                                </NodeSetItem>
                            </NodeSetListCard>
                        </NodeSet>
                    </div>
                );
            })}
        </div>
    );
}
