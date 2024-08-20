import "./StudioSearch.scss";
import { studioSearchInputId, useStudioSearch, studioSearchBackdropId } from "./hooks/useStudioSearch";
import React from "react";
import { Dropdown, DropdownItem, DropdownMenu, Input, Row, DropdownToggle } from "reactstrap";
import classNames from "classnames";
import StudioSearchLegend from "./bits/StudioSearchLegend";
import StudioSearchDatabaseResults from "./bits/StudioSearchDatabaseResults";
import StudioSearchSwitchToDatabaseResults from "./bits/StudioSearchSwitchToDatabaseResults";
import StudioSearchServerResults from "./bits/StudioSearchServerResults";

export default function StudioSearch(props: { menuItems?: menuItem[] }) {
    const { refs, isSearchDropdownOpen, searchQuery, setSearchQuery, matchStatus, results, activeItem, columnWidths } =
        useStudioSearch(props.menuItems);

    return (
        <>
            <Dropdown
                isOpen={isSearchDropdownOpen}
                toggle={() => {}} // handled manually in useStudioSearchMouseEvents() to avoid button click behavior
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
                                `col-md-${columnWidths.database}`
                            )}
                            ref={refs.databaseColumnRef}
                        >
                            <DropdownItem header className="studio-search__database-col__header--sticky">
                                <span className="small-label">Active database</span>
                            </DropdownItem>
                            <StudioSearchDatabaseResults
                                hasDatabaseMatch={matchStatus.hasDatabaseMatch}
                                databaseResults={results.database}
                                activeItem={activeItem}
                            />
                            <StudioSearchSwitchToDatabaseResults
                                hasSwitchToDatabaseMatch={matchStatus.hasSwitchToDatabaseMatch}
                                switchToDatabaseResults={results.switchToDatabase}
                                activeItem={activeItem}
                            />
                        </div>
                        <StudioSearchServerResults
                            serverColumnRef={refs.serverColumnRef}
                            hasServerMatch={matchStatus.hasServerMatch}
                            serverResults={results.server}
                            activeItem={activeItem}
                            colWidth={columnWidths.server}
                        />
                        <StudioSearchLegend />
                    </Row>
                </DropdownMenu>
            </Dropdown>
            {isSearchDropdownOpen && (
                <div id={studioSearchBackdropId} className="modal-backdrop fade show" style={{ zIndex: 1 }} />
            )}
        </>
    );
}
