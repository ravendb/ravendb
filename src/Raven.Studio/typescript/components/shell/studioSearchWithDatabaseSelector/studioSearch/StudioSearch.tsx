import "./StudioSearch.scss";
import { EmptySet } from "components/common/EmptySet";
import StudioSearchDatabaseGroupHeader from "./bits/StudioSearchDatabaseGroupHeader";
import StudioSearchDropdownItem from "./bits/StudioSearchResultItem";
import { StudioSearchResultDatabaseGroup } from "./studioSearchTypes";
import { studioSearchInputId, useStudioSearch } from "./hooks/useStudioSearch";
import React from "react";
import { Col, Dropdown, DropdownItem, DropdownMenu, Input, Row, DropdownToggle } from "reactstrap";
import { todo } from "common/developmentHelper";

todo(
    "Feature",
    "Damian",
    "Filter Settings > Advanced section, Manage Database Group, Query, Patching etc. There are some db-related items in Server column"
);
todo("Feature", "Damian", "Show sharded database icon for sharded database");
todo("Feature", "Damian", "Move Server Environment (shell.html) to DatabaseSelector");

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
        <>
            <Dropdown
                isOpen={isSearchDropdownOpen}
                toggle={toggleDropdown}
                ref={refs.dropdownRef}
                className="omnisearch"
            >
                <DropdownToggle className="omnisearch__toggle">
                    <Input
                        id={studioSearchInputId}
                        innerRef={refs.inputRef}
                        type="search"
                        placeholder="Use Ctrl + K to search"
                        value={searchQuery}
                        onChange={(e) => setSearchQuery(e.target.value)}
                        className="flex-grow-1 omnisearch__input"
                        autoComplete="off"
                    />
                    <DropdownMenu className="omnisearch__results">
                        <Row className="m-0">
                            <Col
                                sm={12}
                                md={matchStatus.hasServerMatch ? 8 : 12}
                                className="omnisearch__database-col p-0"
                            >
                                <DropdownItem header className="omnisearch__database-col__header--sticky">
                                    <span className="small-label">Active database</span>
                                </DropdownItem>
                                {matchStatus.hasDatabaseMatch ? (
                                    Object.keys(results.database)
                                        .filter(
                                            (groupType: StudioSearchResultDatabaseGroup) =>
                                                results.database[groupType].length > 0
                                        )
                                        .map((groupType: StudioSearchResultDatabaseGroup) => (
                                            <div key={groupType} className="omnisearch__database-col__group">
                                                <DropdownItem
                                                    header
                                                    className="omnisearch__database-col__group__header"
                                                >
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
                                    <DropdownItem disabled className="omnisearch__database-col__group pt-0">
                                        <EmptySet compact>No results found</EmptySet>
                                    </DropdownItem>
                                )}

                                {matchStatus.hasSwitchToDatabaseMatch && (
                                    <div className="omnisearch__database-col__group omnisearch__switch-database">
                                        <DropdownItem
                                            header
                                            className="omnisearch__database-col__group__header omnisearch__database-col__group__header--sticky"
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
                            </Col>

                            {matchStatus.hasServerMatch && (
                                <Col sm={12} md={4} className="omnisearch__server-col p-0">
                                    <DropdownItem header className="omnisearch__server-col__header--sticky">
                                        <span className="small-label">Server</span>
                                    </DropdownItem>
                                    <div className="omnisearch__server-col__group">
                                        {results.server.map((item) => (
                                            <StudioSearchDropdownItem
                                                key={item.id}
                                                item={item}
                                                activeItemId={activeItem?.id}
                                            />
                                        ))}
                                    </div>
                                </Col>
                            )}
                            <Col sm={12} className="omnisearch__legend-col p-0">
                                <div className="omnisearch__legend-col__group">
                                    <DropdownItem header className="omnisearch__legend-col__group__header">
                                        <div className="d-flex align-items-center gap-1">
                                            <kbd>↑</kbd> <span>Move up</span>
                                        </div>
                                        <div className="d-flex align-items-center gap-1">
                                            <kbd>↓</kbd> <span>Move down</span>
                                        </div>
                                        <div className="d-flex align-items-center gap-1">
                                            <kbd>ALT</kbd> + <kbd>→</kbd> <span>Move right</span>
                                        </div>
                                        <div className="d-flex align-items-center gap-1">
                                            <kbd>ALT</kbd> + <kbd>←</kbd> <span>Move left</span>
                                        </div>
                                        <div className="d-flex align-items-center gap-1">
                                            <kbd>Enter</kbd> <span>Select</span>
                                        </div>
                                        <div className="d-flex align-items-center gap-1">
                                            <kbd>Esc</kbd> <span>Close</span>
                                        </div>
                                    </DropdownItem>
                                </div>
                            </Col>
                        </Row>
                    </DropdownMenu>
                </DropdownToggle>
            </Dropdown>
            {isSearchDropdownOpen && <div className="modal-backdrop fade show" style={{ zIndex: 1 }} />}
        </>
    );
}
