import React, { useState } from "react";
import { withBootstrap5, withStorybookContexts } from "test/storybookTestUtils";
import { SortDropdown, SortDropdownRadioList, sortItem } from "./SortDropdown";
import { Icon } from "./Icon";

export default {
    title: "Bits/Sort",
    decorators: [withStorybookContexts, withBootstrap5],
};

export function Sort() {
    const sortBy: sortItem[] = [
        { value: "alphabetically", label: "Alphabetically" },
        { value: "creationDate", label: "Creation date" },
        { value: "lastIndexedDate", label: "Last indexed date" },
        { value: "lastQueryDate", label: "Last query date" },
    ];

    const sortDirection: sortItem[] = [
        { value: "asc", label: "Ascending", icon: "arrow-thin-top" },
        { value: "desc", label: "Descending", icon: "arrow-thin-bottom" },
    ];

    const groupBy: sortItem[] = [
        { value: "byCollection", label: "By collection" },
        { value: "none", label: "none" },
    ];

    const [selectedSortBy, setSelectedSortBy] = useState<string>(sortBy[0].value);
    const [selectedSortDirection, setSelectedSortDirection] = useState<string>(sortDirection[0].value);
    const [selectedGroupBy, setSelectedGroupBy] = useState<string>(groupBy[0].value);

    return (
        <div>
            <SortDropdown
                label={
                    <>
                        {sortBy.find((item) => item.value === selectedSortBy).label}{" "}
                        {selectedSortDirection === "asc" ? (
                            <Icon icon="arrow-thin-top" margin="ms-1" />
                        ) : (
                            <Icon icon="arrow-thin-bottom" margin="ms-1" />
                        )}{" "}
                        {selectedGroupBy !== "none" && (
                            <span className="ms-2">{groupBy.find((item) => item.value === selectedGroupBy).label}</span>
                        )}
                    </>
                }
            >
                <SortDropdownRadioList
                    radioOptions={sortBy}
                    label="Sort by"
                    selected={selectedSortBy}
                    setSelected={setSelectedSortBy}
                />
                <SortDropdownRadioList
                    radioOptions={sortDirection}
                    label="Sort direction"
                    selected={selectedSortDirection}
                    setSelected={setSelectedSortDirection}
                />
                <SortDropdownRadioList
                    radioOptions={groupBy}
                    label="Group by"
                    selected={selectedGroupBy}
                    setSelected={setSelectedGroupBy}
                />
            </SortDropdown>
        </div>
    );
}
