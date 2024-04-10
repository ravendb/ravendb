import "./StudioSearch.scss";
import { EmptySet } from "components/common/EmptySet";
import StudioSearchDatabaseGroupHeader from "./bits/StudioSearchDatabaseGroupHeader";
import StudioSearchDropdownItem from "./bits/StudioSearchResultItem";
import { StudioSearchResultDatabaseGroup } from "./studioSearchTypes";
import { studioSearchInputId, useStudioSearch } from "./hooks/useStudioSearch";
import React from "react";
import { Col, Dropdown, DropdownItem, DropdownMenu, Input, Row, DropdownToggle } from "reactstrap";

export default function StudioSearch() {
    const {
        refs,
        isSearchDropdownOpen,
        toggleDropdown,
        searchQuery,
        setSearchQuery,
        matchStatus,
        results,
        activeItem,
    } = useStudioSearch();

    return (
        <Dropdown isOpen={isSearchDropdownOpen} toggle={toggleDropdown} ref={refs.dropdownRef}>
            <DropdownToggle className="d-flex flex-grow-1 p-0">
                <Input
                    id={studioSearchInputId}
                    innerRef={refs.inputRef}
                    type="search"
                    placeholder="Ctrl + K"
                    value={searchQuery}
                    onChange={(e) => setSearchQuery(e.target.value)}
                    className="flex-grow-1"
                />
            </DropdownToggle>
            <DropdownMenu className="studio-search-menu">
                <Row>
                    <Col md={matchStatus.hasServerMatch ? 8 : 12} className="database-column">
                        <DropdownItem header>
                            <span className="text-uppercase">Active database</span>
                        </DropdownItem>
                        {matchStatus.hasDatabaseMatch ? (
                            Object.keys(results.database)
                                .filter(
                                    (groupType: StudioSearchResultDatabaseGroup) =>
                                        results.database[groupType].length > 0
                                )
                                .map((groupType: StudioSearchResultDatabaseGroup) => (
                                    <div key={groupType} className="database-group">
                                        <DropdownItem header>
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
                            <DropdownItem disabled className="database-group">
                                <EmptySet>No results found</EmptySet>
                            </DropdownItem>
                        )}

                        {matchStatus.hasSwitchToDatabaseMatch && (
                            <div className="database-group">
                                <DropdownItem header>
                                    <span className="text-uppercase">Switch active database</span>
                                </DropdownItem>
                                {results.switchToDatabase.map((item) => (
                                    <StudioSearchDropdownItem key={item.id} item={item} activeItemId={activeItem?.id} />
                                ))}
                            </div>
                        )}
                    </Col>

                    {matchStatus.hasServerMatch && (
                        <Col md={4} className="server-column">
                            <DropdownItem header>
                                <span className="text-uppercase">Server</span>
                            </DropdownItem>
                            {results.server.map((item) => (
                                <StudioSearchDropdownItem key={item.id} item={item} activeItemId={activeItem?.id} />
                            ))}
                        </Col>
                    )}
                </Row>
            </DropdownMenu>
        </Dropdown>
    );
}
