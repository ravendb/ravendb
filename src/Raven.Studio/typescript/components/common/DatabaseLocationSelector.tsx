import React, { useState } from "react";
import genUtils from "common/generalUtils";

interface DatabaseLocationSelectorProps {
    locations: databaseLocationSpecifier[];
    selectedLocations: databaseLocationSpecifier[];
    setSelectedLocations: (locations: databaseLocationSpecifier[]) => void;
}

export function DatabaseLocationSelector(props: DatabaseLocationSelectorProps) {
    const { locations, selectedLocations, setSelectedLocations } = props;

    const [ uniqId ] = useState(() => _.uniqueId("location-selector-"));
    
    const toggleSelection = (location: databaseLocationSpecifier) => {
        const selected = selectedLocations.includes(location);
        if (selected) {
            setSelectedLocations(selectedLocations.filter(x => x !== location));
        } else {
            setSelectedLocations([...selectedLocations, location]);
        }
    }

    return (
        <div>
            <ul>
                {locations.map((l, idx) => {
                    const selected = selectedLocations.includes(l);
                    const locationId = uniqId + idx;
                    return (
                        <li className="flex-horizontal" key={locationId}>
                            <div className="checkbox">
                                <input type="checkbox" id={locationId} className="styled" checked={selected}
                                       onChange={() => toggleSelection(l)}/>
                                <label htmlFor={locationId}>
                                    {genUtils.formatLocation(l)}
                                </label>
                            </div>
                        </li>
                    );
                })}
            </ul>
        </div>
    );
}
