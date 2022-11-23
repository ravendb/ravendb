import React, { ChangeEvent, useCallback } from "react";
import { DatabaseFilterCriteria } from "../../../models/databases";
import { CheckboxTriple } from "../../../common/CheckboxTriple";
import { Col, Input, InputGroup, Row } from "reactstrap";

interface DatabasesFilterProps {
    filter: DatabaseFilterCriteria;
    setFilter: React.Dispatch<React.SetStateAction<DatabaseFilterCriteria>>;
    toggleSelectAll: () => void;
    selectionState: checkbox;
}

export function DatabasesFilter(props: DatabasesFilterProps) {
    const { filter, toggleSelectAll, selectionState, setFilter } = props;

    const onSearchTextChange = useCallback(
        (e: ChangeEvent<HTMLInputElement>) => {
            setFilter((f) => ({
                ...f,
                searchText: e.target.value,
            }));
        },
        [setFilter]
    );

    return (
        <Row className="databasesToolbar-filter">
            <Col sm="auto">
                <CheckboxTriple onChanged={toggleSelectAll} state={selectionState} title="Select all or none" />
            </Col>
            <Col>
                <InputGroup>
                    <Input
                        type="text"
                        placeholder="Filter"
                        accessKey="/"
                        title="Filter databases (Alt+/)"
                        value={filter.searchText}
                        onChange={onSearchTextChange}
                    />
                </InputGroup>
            </Col>
            {/* TODO <div className="btn-group">
                <button type="button" className="btn btn-default dropdown-toggle" data-toggle="dropdown"
                        data-bind="css: { 'active': filters.requestedState() !== 'all' }"
                        title="Filter visible databases">
                    <span data-bind="visible: filters.requestedState() === 'all'">Show all</span>
                    <span data-bind="visible: filters.requestedState() === 'local'">Local (Node <span data-bind="text: clusterManager.localNodeTag"/>)</span>
                    <span data-bind="visible: filters.requestedState() === 'online'">Online</span>
                    <span data-bind="visible: filters.requestedState() === 'offline'">Offline</span>
                    <span data-bind="visible: filters.requestedState() === 'disabled'">Disabled</span>
                    <span data-bind="visible: filters.requestedState() === 'errored'">Errored</span>
                    <span data-bind="visible: filters.requestedState() === 'remote'">Remote</span>
                    <span className="caret"/>
                    <span className="sr-only">Toggle Dropdown</span>
                </button>
                <ul className="dropdown-menu">
                    <li className="flex-horizontal" data-bind="click: _.partial(filters.requestedState, 'all')">
                        <a className="flex-grow" href="#"
                           title="Show all databases available in cluster"><span>Show all</span></a>
                        <span className="badge margin-right-sm margin-left-lg"
                              data-bind="text: $root.databases().sortedDatabases().length" />
                    </li>
                    <li className="divider" />
                    <li className="flex-horizontal" data-bind="click: _.partial(filters.requestedState, 'online')">
                        <a className="flex-grow" href="#" title="Show only online databases"><span>Online</span></a>
                        <span className="badge margin-right-sm margin-left-lg"
                              data-bind="text: $root.databasesByState().online" />
                    </li>
                    <li className="flex-horizontal" data-bind="click: _.partial(filters.requestedState, 'offline')">
                        <a className="flex-grow" href="#" title="Show only offline databases"><span>Offline</span></a>
                        <span className="badge margin-right-sm margin-left-lg"
                              data-bind="text: $root.databasesByState().offline" />
                    </li>
                    <li className="flex-horizontal" data-bind="click: _.partial(filters.requestedState, 'disabled')">
                        <a className="flex-grow" href="#" title="Show only disabled databases"><span>Disabled</span></a>
                        <span className="badge margin-right-sm margin-left-lg"
                              data-bind="text: $root.databasesByState().disabled" />
                    </li>
                    <li className="flex-horizontal" data-bind="click: _.partial(filters.requestedState, 'errored')">
                        <a className="flex-grow" href="#" title="Show only errored databases"><span>Errored</span></a>
                        <span className="badge margin-right-sm margin-left-lg"
                              data-bind="text: $root.databasesByState().errored" />
                    </li>
                    <li className="divider"/>
                    <li className="flex-horizontal" data-bind="click: _.partial(filters.requestedState, 'local')">
                        <a className="flex-grow" href="#"
                           title="Show only databases which are relevant for current node">
                            <span>Local (Node <span data-bind="text: clusterManager.localNodeTag"/>)</span>
                        </a>
                        <span className="badge margin-right-sm margin-left-lg"
                              data-bind="text: $root.databases().sortedDatabases().length - $root.databasesByState().remote"/>
                    </li>
                    <li className="flex-horizontal" data-bind="click: _.partial(filters.requestedState, 'remote')">
                        <a className="flex-grow" href="#" title="Show only remote databases"><span>Remote</span></a>
                        <span className="badge margin-right-sm margin-left-lg"
                              data-bind="text: $root.databasesByState().remote" />
                    </li>
                </ul>
            </div>*/}
        </Row>
    );
}
