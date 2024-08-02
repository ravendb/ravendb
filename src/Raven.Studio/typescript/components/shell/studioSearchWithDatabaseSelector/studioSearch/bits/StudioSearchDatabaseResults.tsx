import React from "react";
import { EmptySet } from "components/common/EmptySet";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import StudioSearchDatabaseGroupHeader from "../bits/StudioSearchDatabaseGroupHeader";
import StudioSearchDropdownItem from "../bits/StudioSearchResultItem";
import { StudioSearchResult, StudioSearchResultItem, StudioSearchResultDatabaseGroup } from "../studioSearchTypes";
import { useAppSelector } from "components/store";
import { DropdownItem } from "reactstrap";

export default function StudioSearchDatabaseResults(props: {
    hasDatabaseMatch: boolean;
    databaseResults: StudioSearchResult["database"];
    activeItem?: StudioSearchResultItem;
}) {
    const { hasDatabaseMatch, databaseResults, activeItem } = props;

    const activeDatabase = useAppSelector(databaseSelectors.activeDatabase);

    if (!activeDatabase) {
        return (
            <DropdownItem disabled className="studio-search__database-col__group pt-0">
                <EmptySet compact>
                    No results found. You can select an active database from the selector or by typing it&apos;s name.
                </EmptySet>
            </DropdownItem>
        );
    }

    if (!hasDatabaseMatch) {
        return (
            <DropdownItem disabled className="studio-search__database-col__group pt-0">
                <EmptySet compact>No results found</EmptySet>
            </DropdownItem>
        );
    }

    const matchedKeys = Object.keys(databaseResults).filter(
        (groupType: StudioSearchResultDatabaseGroup) => databaseResults[groupType].length > 0
    );

    return matchedKeys.map((groupType: StudioSearchResultDatabaseGroup) => (
        <div key={groupType} className="studio-search__database-col__group">
            <DropdownItem header className="studio-search__database-col__group__header">
                <StudioSearchDatabaseGroupHeader groupType={groupType} />
            </DropdownItem>
            {databaseResults[groupType].map((item) => (
                <StudioSearchDropdownItem key={item.id} item={item} activeItemId={activeItem?.id} isCapitalized />
            ))}
        </div>
    ));
}
