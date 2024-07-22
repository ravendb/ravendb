import React from "react";
import StudioSearchDropdownItem from "../bits/StudioSearchResultItem";
import { StudioSearchResult, StudioSearchResultItem } from "../studioSearchTypes";
import { DropdownItem } from "reactstrap";

export default function StudioSearchSwitchToDatabaseResults(props: {
    hasSwitchToDatabaseMatch: boolean;
    switchToDatabaseResults: StudioSearchResult["switchToDatabase"];
    activeItem?: StudioSearchResultItem;
}) {
    const { hasSwitchToDatabaseMatch, switchToDatabaseResults, activeItem } = props;

    if (!hasSwitchToDatabaseMatch) {
        return null;
    }

    return (
        <div className="studio-search__database-col__group studio-search__switch-database">
            <DropdownItem
                header
                className="studio-search__database-col__group__header studio-search__database-col__group__header--sticky"
            >
                <span className="small-label">Switch active database</span>
            </DropdownItem>
            {switchToDatabaseResults.map((item) => (
                <StudioSearchDropdownItem key={item.id} item={item} activeItemId={activeItem?.id} />
            ))}
        </div>
    );
}
