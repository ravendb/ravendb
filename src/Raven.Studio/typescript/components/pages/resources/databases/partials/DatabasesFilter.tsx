import React, { ChangeEvent } from "react";
import { DatabaseFilterCriteria } from "components/models/databases";
import { Col, Input, Row } from "reactstrap";
import { useAppDispatch } from "components/store";
import { filterTextSet } from "components/common/shell/databasesSlice";
import { InputItem, MultiToggle } from "components/common/MultiToggle";
import "./DatabasesFilter.scss";

interface DatabasesFilterProps {
    filter: DatabaseFilterCriteria;
}

export function DatabasesFilter(props: DatabasesFilterProps) {
    const { filter } = props;

    const dispatch = useAppDispatch();

    const onSearchTextChange = (e: ChangeEvent<HTMLInputElement>) => {
        dispatch(filterTextSet(e.target.value));
    };

    const filterByStateList: InputItem[] = [
        { value: "all", label: "All", count: 1 },
        { value: "online", label: "Online", count: 1 },
        { value: "offline", label: "Offline", count: 1 },
        { value: "disabled", label: "Disabled", count: 1 },
        { value: "errored", label: "Errored", count: 1 },
        { value: "local", label: "Local", count: 1 },
        { value: "remote", label: "Remote", count: 1 },
    ];

    return (
        <>
            <Row className="d-flex align-items-end mb-3">
                <Col>
                    <div className="small-label ms-1 mb-1">Filter by name</div>
                    <Input
                        type="text"
                        accessKey="/"
                        placeholder="e.g. database1"
                        title="Filter databases (Alt+/)"
                        value={filter.searchText}
                        onChange={onSearchTextChange}
                        className="filtering-input"
                    />
                </Col>
                <Col>
                    <MultiToggle inputList={filterByStateList} label="Filter by state"></MultiToggle>
                </Col>
                <Col></Col>
            </Row>
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
        </>
    );
}

/* TODO

MODEL:
 filters = {
        searchText: ko.observable<string>(),
        requestedState: ko.observable<filterState>('all')
    };
     const filters = this.filters;

        filters.searchText.throttle(200).subscribe(() => this.filterDatabases());
        filters.requestedState.subscribe(() => this.filterDatabases());

        
        this.databasesByState = ko.pureComputed(() => {
            const databases = this.databases().sortedDatabases();
            
            const result: Record<databaseState, number> = {
                errored: 0,
                disabled: 0,
                offline: 0,
                online: 0,
                remote: 0
            };

            for (const database of databases) {
                if (database.hasLoadError()) {
                    result.errored++;
                //TODO } else if (!this.isLocalDatabase(database.name)) {
                //TODO:     result.remote++;
                } else if (database.disabled()) {
                    result.disabled++;
                } else if (database.online()) {
                    result.online++;
                } else {
                    result.offline++;
                }
            }
            
            return result;
        });

private filterDatabases(): void {
    /* TODO
    const filters = this.filters;
    let searchText = filters.searchText();
    const hasSearchText = !!searchText;

    if (hasSearchText) {
        searchText = searchText.toLowerCase();
    }

    const matchesFilters = (db: databaseInfo): boolean => {
        const state = filters.requestedState();
        const nodeTag = this.clusterManager.localNodeTag();
        
        const matchesOnline = state === 'online' && db.online();
        const matchesDisabled = state === 'disabled' && db.disabled();
        const matchesErrored = state === 'errored' && db.hasLoadError();
        const matchesOffline = state === 'offline' && (!db.online() && !db.disabled() && !db.hasLoadError() && db.isLocal(nodeTag));
        
        const matchesLocal = state === 'local' && db.isLocal(nodeTag);
        const matchesRemote = state === 'remote' && !db.isLocal(nodeTag);
        const matchesAll = state === 'all';
        
        const matchesText = !hasSearchText || db.name.toLowerCase().indexOf(searchText) >= 0;
        
        return matchesText &&
            (matchesOnline || matchesDisabled || matchesErrored || matchesOffline || matchesLocal || matchesRemote || matchesAll);
    };

    const databases = this.databases();
    databases.sortedDatabases().forEach(db => {
        const matches = matchesFilters(db);
        db.filteredOut(!matches);

        if (!matches) {
            this.selectedDatabases.remove(db.name);
        }
    });
     
}
 */
