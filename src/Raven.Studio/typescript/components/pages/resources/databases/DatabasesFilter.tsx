import React from "react";


export function DatabasesFilter() {
    return null as JSX.Element; //TODO:
    /* TODO:
    return (
        <div className="databasesToolbar-filter">
            <div className="checkbox checkbox-primary checkbox-inline align-checkboxes">
                <input type="checkbox" className="styled"
                       data-bind="checkboxTriple: selectionState, event: { change: toggleSelectAll }" />
                    <label />
            </div>
            <div className="input-group">
                <input type="text" className="form-control" placeholder="Filter" accessKey="/"
                       title="Filter databases (Alt+/)" data-bind="textInput: filters.searchText" />
            </div>
            <div className="btn-group">
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
            </div>
        </div>
    )
     */
}
