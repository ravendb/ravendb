import "./StudioSearch.scss";
import { EmptySet } from "components/common/EmptySet";
import StudioSearchDatabaseGroupHeader from "./bits/StudioSearchDatabaseGroupHeader";
import StudioSearchDropdownItem from "./bits/StudioSearchResultItem";
import { StudioSearchResultDatabaseGroup } from "./studioSearchTypes";
import { studioSearchInputId, useStudioSearch } from "./hooks/useStudioSearch";
import React from "react";
import { Dropdown, DropdownItem, DropdownMenu, Input, Row, DropdownToggle } from "reactstrap";
import classNames from "classnames";
import StudioSearchLegend from "components/shell/studioSearchWithDatabaseSelector/studioSearch/bits/StudioSearchLegend";

export default function StudioSearch(props: { menuItems: menuItem[] }) {
    const {
        refs,
        isSearchDropdownOpen,
        toggleDropdown,
        searchQuery,
        setSearchQuery,
        matchStatus,
        results,
        activeItem,
    } = useStudioSearch(props.menuItems);

    return (
        <>
            <Dropdown
                isOpen={isSearchDropdownOpen}
                toggle={toggleDropdown}
                ref={refs.dropdownRef}
                className="studio-search"
            >
                <DropdownToggle className="studio-search__toggle">
                    <Input
                        id={studioSearchInputId}
                        innerRef={refs.inputRef}
                        type="search"
                        placeholder="Use Ctrl + K to search"
                        value={searchQuery}
                        onChange={(e) => setSearchQuery(e.target.value)}
                        className="flex-grow-1 studio-search__input align-self-stretch"
                        autoComplete="off"
                    />
                </DropdownToggle>
                <DropdownMenu className="studio-search__results">
                    <Row className="m-0">
                        <div
                            className={classNames(
                                "col-sm-12 studio-search__database-col p-0",
                                `col-md-${matchStatus.hasServerMatch ? 8 : 12}`
                            )}
                            ref={refs.databaseColumnRef}
                        >
                            <DropdownItem header className="studio-search__database-col__header--sticky">
                                <span className="small-label">Active database</span>
                            </DropdownItem>
                            {matchStatus.hasDatabaseMatch ? (
                                Object.keys(results.database)
                                    .filter(
                                        (groupType: StudioSearchResultDatabaseGroup) =>
                                            results.database[groupType].length > 0
                                    )
                                    .map((groupType: StudioSearchResultDatabaseGroup) => (
                                        <div key={groupType} className="studio-search__database-col__group">
                                            <DropdownItem header className="studio-search__database-col__group__header">
                                                <StudioSearchDatabaseGroupHeader groupType={groupType} />
                                            </DropdownItem>
                                            {results.database[groupType].map((item) => (
                                                <StudioSearchDropdownItem
                                                    key={item.id}
                                                    item={item}
                                                    activeItemId={activeItem?.id}
                                                />
                                            ))}
                                        </div>
                                    ))
                            ) : (
                                <DropdownItem disabled className="studio-search__database-col__group pt-0">
                                    <EmptySet compact>No results found</EmptySet>
                                </DropdownItem>
                            )}
                            {matchStatus.hasSwitchToDatabaseMatch && (
                                <div className="studio-search__database-col__group studio-search__switch-database">
                                    <DropdownItem
                                        header
                                        className="studio-search__database-col__group__header studio-search__database-col__group__header--sticky"
                                    >
                                        <span className="small-label">Switch active database</span>
                                    </DropdownItem>
                                    {results.switchToDatabase.map((item) => (
                                        <StudioSearchDropdownItem
                                            key={item.id}
                                            item={item}
                                            activeItemId={activeItem?.id}
                                        />
                                    ))}
                                </div>
                            )}
                        </div>
                        {matchStatus.hasServerMatch && (
                            <div
                                className="col-md-4 col-sm-12 studio-search__server-col p-0"
                                ref={refs.serverColumnRef as any}
                            >
                                <DropdownItem header className="studio-search__server-col__header--sticky">
                                    <span className="small-label">Server</span>
                                </DropdownItem>
                                <div className="studio-search__server-col__group">
                                    {results.server.map((item) => (
                                        <StudioSearchDropdownItem
                                            key={item.id}
                                            item={item}
                                            activeItemId={activeItem?.id}
                                        />
                                    ))}
                                </div>
                            </div>
                        )}
                        <StudioSearchLegend />
                    </Row>
                </DropdownMenu>
            </Dropdown>
            {isSearchDropdownOpen && <div className="modal-backdrop fade show" style={{ zIndex: 1 }} />}
        </>
    );
}
