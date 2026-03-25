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
    searchQuery: string;
}) {
    const { hasDatabaseMatch, databaseResults, activeItem, searchQuery } = props;

    const activeDatabase = useAppSelector(databaseSelectors.activeDatabase);

    if (!activeDatabase) {
        return (
            <Dropdown.Item disabled className="studio-search__database-col__group pt-0">
                <EmptySet compact>
                    No active database
                    <br />
                    Choose a database from the selector or by typing its name
                </EmptySet>
            </Dropdown.Item>
        );
    }

    if (!searchQuery) {
        return (
            <Dropdown.Item disabled className="studio-search__database-col__group pt-0">
                <EmptySet compact>
                    Search <b>{activeDatabase.name}</b>
                    <br />
                    Type to find results in this database
                </EmptySet>
            </Dropdown.Item>
        );
    }

    if (!hasDatabaseMatch) {
        return (
            <Dropdown.Item disabled className="studio-search__database-col__group pt-0">
                <EmptySet compact>
                    <span>
                        No matches for <b>{props.searchQuery}</b> in <b>{activeDatabase.name}</b>
                    </span>
                    <br />
                    <span>Refine your search or Ask AI</span>
                </EmptySet>
            </Dropdown.Item>
        );
    }

    return getSortedKeys(databaseResults).map((groupType: StudioSearchResultDatabaseGroup) => (
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

function getSortedKeys(databaseResults: StudioSearchResult["database"]): string[] {
    let keys = Object.keys(databaseResults).filter(
        (groupType: StudioSearchResultDatabaseGroup) => databaseResults[groupType].length > 0
    );

    // move revisions after documents
    if (keys.includes("revisions") && keys.includes("documents")) {
        keys = keys.filter((x) => x !== "revisions");
        keys.splice(keys.indexOf("documents") + 1, 0, "revisions");
    }

    return keys;
}
