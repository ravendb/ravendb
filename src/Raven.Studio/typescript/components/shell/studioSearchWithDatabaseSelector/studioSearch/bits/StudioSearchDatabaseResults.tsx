import React from "react";
import { EmptySet } from "components/common/EmptySet";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import StudioSearchDatabaseGroupHeader from "../bits/StudioSearchDatabaseGroupHeader";
import StudioSearchDropdownItem from "../bits/StudioSearchResultItem";
import { StudioSearchResult, StudioSearchResultDatabaseGroup, StudioSearchResultItem } from "../studioSearchTypes";
import { useAppSelector } from "components/store";
import Dropdown from "react-bootstrap/Dropdown";

export default function StudioSearchDatabaseResults(props: {
    hasDatabaseMatch: boolean;
    databaseResults: StudioSearchResult["database"];
    activeItem?: StudioSearchResultItem;
}) {
    const { hasDatabaseMatch, databaseResults, activeItem } = props;

    const activeDatabase = useAppSelector(databaseSelectors.activeDatabase);

    if (!activeDatabase) {
        return (
            <Dropdown.Item disabled className="studio-search__database-col__group pt-0">
                <EmptySet compact>
                    No results found.
                    <br />
                    You can select an active database from the selector or by typing its name.
                </EmptySet>
            </Dropdown.Item>
        );
    }

    if (!hasDatabaseMatch) {
        return (
            <Dropdown.Item disabled className="studio-search__database-col__group pt-0">
                <EmptySet compact>No results found</EmptySet>
            </Dropdown.Item>
        );
    }

    const matchedKeys = Object.keys(databaseResults).filter(
        (groupType: StudioSearchResultDatabaseGroup) => databaseResults[groupType].length > 0
    );

    return matchedKeys.map((groupType: StudioSearchResultDatabaseGroup) => (
        <div key={groupType} className="studio-search__database-col__group">
            <Dropdown.Header className="studio-search__database-col__group__header">
                <StudioSearchDatabaseGroupHeader groupType={groupType} />
            </Dropdown.Header>
            {databaseResults[groupType].map((item) => (
                <StudioSearchDropdownItem key={item.id} item={item} activeItemId={activeItem?.id} />
            ))}
        </div>
    ));
}
