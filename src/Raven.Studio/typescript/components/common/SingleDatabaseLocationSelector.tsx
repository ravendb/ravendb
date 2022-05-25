import React, { useState } from "react";
import genUtils from "common/generalUtils";

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
            <ul>
                {locations.map((l, idx) => {
                    const selected = selectedLocation === l;
                    const locationId = uniqId + idx;
                    return (
                        <li className="flex-horizontal" key={locationId}>
                            <div className="radio radio-default">
                                <input
                                    type="radio"
                                    id={locationId}
                                    checked={selected}
                                    onChange={() => toggleSelection(l)}
                                />
                                <label htmlFor={locationId}>{genUtils.formatLocation(l)}</label>
                            </div>
                        </li>
                    );
                })}
            </ul>
        </div>
    );
}
