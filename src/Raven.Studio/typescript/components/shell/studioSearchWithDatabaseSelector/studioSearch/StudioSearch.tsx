import "./StudioSearch.scss";
import { studioSearchBackdropId, studioSearchInputId, useStudioSearch } from "./hooks/useStudioSearch";
import React from "react";
import Form from "react-bootstrap/Form";
import classNames from "classnames";
import StudioSearchLegend from "./bits/StudioSearchLegend";
import StudioSearchDatabaseResults from "./bits/StudioSearchDatabaseResults";
import StudioSearchSwitchToDatabaseResults from "./bits/StudioSearchSwitchToDatabaseResults";
import StudioSearchServerResults from "./bits/StudioSearchServerResults";
import Dropdown from "react-bootstrap/Dropdown";
import { useOS } from "hooks/useOS";
import { Icon } from "components/common/Icon";
import { aiAssistantSelectors } from "components/common/shell/aiAssistantSlice";
import { useAppSelector } from "components/store";
import Row from "react-bootstrap/Row";
import Button from "react-bootstrap/Button";

export default function StudioSearch(props: { menuItems?: menuItem[] }) {
    const { refs, isSearchDropdownOpen, searchQuery, setSearchQuery, matchStatus, results, activeItem, handleAskAi } =
        useStudioSearch(props.menuItems);

    const isAiAssistantDisabled = useAppSelector(aiAssistantSelectors.isDisabled);
    const operatingSystem = useOS();

    const isAskAiVisible = !isAiAssistantDisabled && searchQuery;

    return (
        <>
            <Dropdown
                show={isSearchDropdownOpen}
                onToggle={() => {}} // handled manually in useStudioSearchMouseEvents() to avoid button click behavior
                className="studio-search"
            >
                <Dropdown.Toggle variant="secondary" className="studio-search__toggle">
                    <Form.Control
                        id={studioSearchInputId}
                        ref={refs.inputRef}
                        type="search"
                        placeholder={operatingSystem === "MacOS" ? "Use ⌘ + K to search" : "Use Ctrl + K to search"}
                        value={searchQuery}
                        onChange={(e) => setSearchQuery(e.target.value)}
                        className="flex-grow-1 studio-search__input align-self-stretch pe-2"
                        autoComplete="off"
                    />
                    {isAskAiVisible && (
                        <Button
                            onClick={handleAskAi}
                            className="studio-search__ask-ai-button"
                            id="ask-ai"
                            title={"Ask AI about " + searchQuery}
                            variant="secondary"
                        >
                            <Icon icon="ask-ai" className="ai-gradient" />
                            Ask AI
                        </Button>
                    )}
                </Dropdown.Toggle>
                <Dropdown.Menu className="studio-search__results">
                    <Row className="m-0">
                        <div
                            className={classNames(
                                "col-sm-12 studio-search__database-col p-0",
                                `col-md-${matchStatus.hasServerMatch ? 7 : 12}`
                            )}
                            ref={refs.databaseColumnRef}
                        >
                            <Dropdown.Header className="studio-search__database-col__header--sticky">
                                <span className="small-label">Active database</span>
                            </Dropdown.Header>

                            <StudioSearchDatabaseResults
                                hasDatabaseMatch={matchStatus.hasDatabaseMatch}
                                databaseResults={results.database}
                                activeItem={activeItem}
                                searchQuery={searchQuery}
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
                        />
                        <StudioSearchLegend />
                    </Row>
                </Dropdown.Menu>
            </Dropdown>
            {isSearchDropdownOpen && (
                <div id={studioSearchBackdropId} className="modal-backdrop fade show" style={{ zIndex: 1 }} />
            )}
        </>
    );
}
