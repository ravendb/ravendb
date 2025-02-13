import React, { useState } from "react";
import { withBootstrap5, withStorybookContexts } from "test/storybookTestUtils";
import { SortDropdown, SortDropdownRadioList, sortItem } from "./SortDropdown";
import { Icon } from "./Icon";

export default {
    title: "Bits/Sort",
    decorators: [withStorybookContexts, withBootstrap5],
    parameters: {
        design: {
            type: "figma",
            url: "https://www.figma.com/design/ITHbe2U19Ok7cjbEzYa4cb/Design-System-RavenDB-Studio?node-id=8-3858&t=ipd4AuT8v0is7yTp-4",
        },
    },
};

type SortBy = "Alphabetically" | "Creation date";
type SortDirection = "Ascending" | "Descending";
type GroupBy = "Collection" | "None";

export function Sort() {
    const sortBy: sortItem<SortBy>[] = [
        { value: "Alphabetically", label: "Alphabetically" },
        { value: "Creation date", label: "Creation date" },
    ];

    const sortDirection: sortItem<SortDirection>[] = [
        { value: "Ascending", label: "Ascending", icon: "arrow-thin-top" },
        { value: "Descending", label: "Descending", icon: "arrow-thin-bottom" },
    ];

    const groupBy: sortItem<GroupBy>[] = [
        { value: "Collection", label: "Collection" },
        { value: "None", label: "None" },
    ];

    const [selectedSortBy, setSelectedSortBy] = useState<SortBy>("Alphabetically");
    const [selectedSortDirection, setSelectedSortDirection] = useState<SortDirection>("Descending");
    const [selectedGroupBy, setSelectedGroupBy] = useState<GroupBy>("None");

    return (
        <div>
            <SortDropdown
                label={
                    <>
                        {sortBy.find((item) => item.value === selectedSortBy).label}{" "}
                        {selectedSortDirection === "Ascending" ? (
                            <Icon icon="arrow-thin-bottom" margin="ms-1" />
                        ) : (
                            <Icon icon="arrow-thin-top" margin="ms-1" />
                        )}
                        {selectedGroupBy !== "None" && (
                            <span className="ms-2">{groupBy.find((item) => item.value === selectedGroupBy).label}</span>
                        )}
                    </>
                }
            >
                <SortDropdownRadioList<SortBy>
                    radioOptions={sortBy}
                    label="Sort by"
                    selected={selectedSortBy}
                    setSelected={setSelectedSortBy}
                />
                <SortDropdownRadioList<SortDirection>
                    radioOptions={sortDirection}
                    label="Sort direction"
                    selected={selectedSortDirection}
                    setSelected={setSelectedSortDirection}
                />
                <SortDropdownRadioList<GroupBy>
                    radioOptions={groupBy}
                    label="Group by"
                    selected={selectedGroupBy}
                    setSelected={setSelectedGroupBy}
                />
            </SortDropdown>
        </div>
    );
}
