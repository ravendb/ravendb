import React, { MouseEvent, useState } from "react";
import classNames from "classnames";
import IndexPriority = Raven.Client.Documents.Indexes.IndexPriority;
import { IndexNodeInfo, IndexSharedInfo } from "../../../models/indexes";
import IndexLockMode = Raven.Client.Documents.Indexes.IndexLockMode;
import { useAppUrls } from "../../../hooks/useAppUrls";
import IndexUtils from "../../../utils/IndexUtils";

interface IndexPanelProps {
    index: IndexSharedInfo;
    setPriority: (priority: IndexPriority) => Promise<void>;
    setLockMode: (lockMode: IndexLockMode) => Promise<void>;
    deleteIndex: () => Promise<void>;
    selected: boolean;
    toggleSelection: () => void;
}

export function IndexPanel(props: IndexPanelProps) {
    const { index, selected, toggleSelection } = props;

    const [updatingLocalPriority, setUpdatingLocalPriority] = useState(false);
    const [updatingLockMode, setUpdatingLockMode] = useState(false);
    
    const setPriority = async (e: MouseEvent, priority: IndexPriority) => {
        e.preventDefault();
        setUpdatingLocalPriority(true);
        try {
            await props.setPriority(priority);
        } finally {
            setUpdatingLocalPriority(false);
        }
    }
    
    const setLockMode = async (e: MouseEvent, lockMode: IndexLockMode) => {
        e.preventDefault();
        setUpdatingLockMode(true);
        try {
            await props.setLockMode(lockMode);
        } finally {
            setUpdatingLockMode(false);
        }
    }
    
    const deleteIndex = async (e: MouseEvent) => {
        e.preventDefault();
        return props.deleteIndex();
    }
    
    const urls = useAppUrls();
    const queryUrl = urls.query(index.name)();
    const termsUrl = urls.terms(index.name)();
    const editUrl = urls.editIndex(index.name)();
    
    return (
        <div className="sidebyside-indexes">
            <div className="panel panel-state panel-hover index" data-bind="css: { 'has-replacement': replacement }">
                <div className="padding padding-sm js-index-template" id={indexUniqueId(index)}>
                    <div className={classNames("state", badgeClass(index))} data-state-text={badgeText(index)} />
                    <div className="row">
                        <div className="col-xs-12 col-sm-6 col-xl-4 info-container">
                            <div className="flex-horizontal">
                                <div className="checkbox" data-bind="requiredAccess: 'DatabaseReadWrite'">
                                    <input type="checkbox" className="styled" checked={selected} onChange={toggleSelection} />
                                    <label/>
                                </div>
                                <h3 className="index-name flex-grow">
                                    <a href={editUrl} title={index.name}>{index.name}</a>
                                </h3>
                                { index.sourceType === "Counters" && (
                                    <i className="icon-new-counter" title="Index source: Counters" />
                                )}
                                { index.sourceType === "TimeSeries" && (
                                    <i className="icon-timeseries" title="Index source: Time Series" />
                                )}
                                { index.sourceType === "Documents" && (
                                    <i className="icon-documents" title="Index source: Documents" />
                                )}
                            </div>
                            <div className="flex-horizontal clear-left index-info nospacing">
                                <div className="index-type-icon" data-placement="right" data-toggle="tooltip"
                                     data-animation="true" data-html="true"
                                     data-bind="tooltipText: mapReduceIndexInfoTooltip">
                                    { index.reduceOutputCollectionName && !index.patternForReferencesToReduceOutputCollection && (
                                        <span>
                                            <i className="icon-output-collection" />
                                        </span>
                                    )}
                                    { index.patternForReferencesToReduceOutputCollection && (
                                        <span><i className="icon-reference-pattern"/></span>
                                    )}
                                </div>
                                <div className="index-type">
                                    <span>{IndexUtils.formatType(index.type)}</span>
                                    { /* TODO
                                    
                                    <span data-bind="visible: replacement" className="margin-left margin-left-sm"><span
                                        className="label label-warning">OLD</span></span>
                                    <span data-bind="visible: parent" className="margin-left margin-left-sm"><span
                                        className="label label-warning">NEW</span></span> */ }
                                </div>
                            </div>
                        </div>
                        { !IndexUtils.isFaulty(index) && (
                            <div className="col-xs-12 col-sm-12 col-xl-5 vertical-divider properties-container">
                                <div className="properties-item state-selector">
                                    <span className="properties-label">State:</span>
                                    <div className="btn-group properties-value">
                                        <button type="button" className="btn set-size dropdown-toggle" data-toggle="dropdown"
                                                data-bind="css: { 'btn-spinner': _.includes($root.spinners.localState(), name) },
                                               enable: $root.globalIndexingStatus() === 'Running'  && !_.includes($root.spinners.localState(), name),
                                               requiredAccess: 'DatabaseReadWrite', requiredAccessOptions: { strategy: 'disable' }">
                                            { (IndexUtils.isPausedState(index) && !IndexUtils.isErrorState(index)) && (
                                                <span className="text-warning">
                                                <i className="icon-cancel" />
                                                <span>Paused until restart</span>
                                            </span>
                                            ) }
                                            { IndexUtils.isNormalState(index) && (
                                                <span>
                                                <i className="icon-check" />
                                                <span>Normal</span>
                                            </span>
                                            )}
                                            { IndexUtils.isIdleState(index) && (
                                                <span>
                                                <i className="icon-coffee" />
                                                <span>Idle</span>
                                            </span>
                                            )}
                                            { IndexUtils.isDisabledState(index) && (
                                                <span className="text-danger">
                                                <i className="icon-cancel" />
                                                <span>Indexing disabled</span>
                                            </span>
                                            )}
                                            { IndexUtils.isErrorState(index) && (
                                                <span className="text-danger">
                                                <i className="icon-danger" />
                                                <span>Error</span>
                                            </span>
                                            )}
                                            <span className="caret"/>
                                        </button>
                                        <ul className="dropdown-menu">
                                            <li data-bind="visible: canBeEnabled() && $root.isCluster()">
                                                <a href="#" data-bind="click: _.partial($root.enableIndex, $data, true)"
                                                   title="Enable indexing on ALL cluster nodes">
                                                    <i className="icon-play" />
                                                    <span>Enable indexing - Cluster wide</span>
                                                </a>
                                            </li>
                                            <li data-bind="visible: canBeEnabled()">
                                                <a href="#"
                                                   data-bind="click: _.partial($root.enableIndex, $data, false), attr: { title: 'Enable indexing on node ' + $root.localNodeTag() }">
                                                    <i className="icon-play" />
                                                    <span data-bind="text: $root.isCluster() ? 'Enable indexing - Local node' : 'Enable indexing'"/>
                                                </a>
                                            </li>
                                            <li data-bind="visible: canBeDisabled() && $root.isCluster()">
                                                <a href="#" data-bind="click: _.partial($root.disableIndex, $data, true)"
                                                   title="Disable indexing on ALL cluster nodes">
                                                    <i className="icon-cancel"/>
                                                    <span>Disable indexing - Cluster wide</span>
                                                </a>
                                            </li>
                                            <li data-bind="visible: canBeDisabled()">
                                                <a href="#"
                                                   data-bind="click: _.partial($root.disableIndex, $data, false), attr: { title: 'Disable indexing on node ' + $root.localNodeTag() }">
                                                    <i className="icon-cancel"/>
                                                    <span data-bind="text: $root.isCluster() ? 'Disable indexing - Local node' : 'Disable indexing'"/>
                                                </a>
                                            </li>
                                            <li data-bind="visible: canBePaused()">
                                                <a href="#" className="text-warning" data-bind="click: $root.pauseUntilRestart"
                                                   title="Pause until restart">
                                                    <i className="icon-pause"/>
                                                    <span>Pause indexing until restart</span>
                                                </a>
                                            </li>
                                            <li data-bind="visible: canBeResumed()">
                                                <a href="#" className="text-success" data-bind="click: $root.resumeIndexing"
                                                   title="Resume indexing">
                                                    <i className="icon-play"/>
                                                    <span>Resume indexing</span>
                                                </a>
                                            </li>
                                        </ul>
                                    </div>
                                </div>
                                <div className="properties-item priority" data-bind="if: !isSideBySide()">
                                    <span className="properties-label">Priority:</span>
                                    <div className="btn-group properties-value">
                                        <button type="button" className={classNames("btn set-size dropdown-toggle", { "btn-spinner": updatingLocalPriority, "enable": !updatingLocalPriority })}
                                                data-toggle="dropdown"
                                                data-bind="requiredAccess: 'DatabaseReadWrite', requiredAccessOptions: { strategy: 'disable' }">
                                            { index.priority === "Normal" && (
                                                <span>
                                                <i className="icon-check"/>
                                                <span>Normal</span>
                                            </span>
                                            )}
                                            { index.priority === "Low" && (
                                                <span>
                                                <i className="icon-coffee"/>
                                                <span>Low</span>
                                            </span>
                                            )}
                                            { index.priority === "High" && (
                                                <span>
                                                <i className="icon-force"/>
                                                <span>High</span>
                                            </span>
                                            )}
                                            <span className="caret"/>
                                        </button>
                                        <ul className="dropdown-menu">
                                            <li>
                                                <a href="#" onClick={e => setPriority(e, "Low")} title="Low">
                                                    <i className="icon-coffee"/><span>Low</span>
                                                </a>
                                            </li>
                                            <li>
                                                <a href="#" onClick={e => setPriority(e, "Normal")} title="Normal">
                                                    <i className="icon-check"/><span>Normal</span>
                                                </a>
                                            </li>
                                            <li>
                                                <a href="#" onClick={e => setPriority(e, "High")} title="High">
                                                    <i className="icon-force"/><span>High</span>
                                                </a>
                                            </li>
                                        </ul>
                                    </div>
                                </div>
                                <div className="properties-item mode"
                                     data-bind="css: { 'hidden': type() === 'AutoMap' || type() === 'AutoMapReduce' || isSideBySide() }">
                                    <span className="properties-label">Mode:</span>
                                    <div className="btn-group properties-value">
                                        <button type="button" className={classNames("btn set-size dropdown-toggle", { "btn-spinner": updatingLockMode, enable: !updatingLockMode })} data-toggle="dropdown"
                                                data-bind="requiredAccess: 'DatabaseReadWrite', requiredAccessOptions: { strategy: 'disable' }">
                                            {index.lockMode === "Unlock" && (
                                                <span>
                                                <i className="icon-unlock"/><span>Unlocked</span>
                                            </span>
                                            )}
                                            {index.lockMode === "LockedIgnore" && (
                                                <span>
                                                <i className="icon-lock"/><span>Locked</span>
                                            </span>
                                            )}
                                            {index.lockMode === "LockedError" && (
                                                <span>
                                                <i className="icon-lock-error"/><span>Locked (Error)</span>
                                            </span>
                                            )}
                                            <span className="caret"/>
                                        </button>
                                        <ul className="dropdown-menu">
                                            <li>
                                                <a href="#" onClick={e => setLockMode(e, "Unlock")} title="Unlocked: The index is unlocked for changes; apps can modify it, e.g. via IndexCreation.CreateIndexes().">
                                                    <i className="icon-unlock"/>
                                                    <span>Unlock</span>
                                                </a>
                                            </li>
                                            <li className="divider"/>
                                            <li>
                                                <a href="#" onClick={e => setLockMode(e, "LockedIgnore")}
                                                   title="Locked: The index is locked for changes; apps cannot modify it. Programmatic attempts to modify the index will be ignored.">
                                                    <i className="icon-lock"/>
                                                    <span>Lock</span>
                                                </a>
                                            </li>
                                            <li>
                                                <a href="#" onClick={e => setLockMode(e, "LockedError")}
                                                   title="Locked + Error: The index is locked for changes; apps cannot modify it. An error will be thrown if an app attempts to modify it.">
                                                    <i className="icon-lock-error"/>
                                                    <span>Lock (Error)</span>
                                                </a>
                                            </li>
                                        </ul>
                                    </div>
                                </div>
                            </div>
                        )}
                        
                        <div className="col-xs-12 col-sm-6 col-xl-3 actions-container">
                            <div className="actions">
                                <div className="btn-toolbar pull-right-sm" role="toolbar">
                                    { !IndexUtils.isFaulty(index) && (
                                        <div className="btn-group" role="group">
                                            <a className="btn btn-default" href={queryUrl}>
                                                <i className="icon-search"/><span>Query</span></a>
                                            <button type="button" className="btn btn-default dropdown-toggle"
                                                    data-toggle="dropdown" aria-haspopup="true" aria-expanded="false">
                                                <span className="caret"/>
                                                <span className="sr-only">Toggle Dropdown</span>
                                            </button>
                                            <ul className="dropdown-menu">
                                                <li>
                                                    <a href={termsUrl}>
                                                        <i className="icon-terms"/> Terms
                                                    </a>
                                                </li>
                                            </ul>
                                        </div>
                                    )}
                                    
                                    <div className="btn-group" role="group">
                                        { !IndexUtils.isAutoIndex(index) && (
                                            <a className="btn btn-default" href={editUrl}
                                               data-bind=" visible: !isAutoIndex() && !$root.isReadOnlyAccess()"
                                               title="Edit index"><i className="icon-edit"/></a>
                                        )}
                                        { IndexUtils.isAutoIndex(index) && (
                                            <a className="btn btn-default" href={editUrl}
                                               data-bind=", visible: isAutoIndex() || $root.isReadOnlyAccess()"
                                               title="View index"><i className="icon-preview"/></a>
                                        )}
                                    </div>
                                    { IndexUtils.isFaulty(index) && (
                                        <div className="btn-group" role="group">
                                            <button className="btn btn-default" data-bind="click: $root.openFaultyIndex"
                                                    title="Open index"><i className="icon-arrow-filled-up"/></button>
                                        </div>    
                                    )}
                                    
                                    <div className="btn-group" role="group">
                                        <button className="btn btn-warning"
                                                data-bind="click: $root.resetIndex, requiredAccess: 'DatabaseReadWrite'"
                                                title="Reset index (rebuild)"><i className="icon-reset-index"/></button>
                                        <button className="btn btn-danger" onClick={deleteIndex}
                                                data-bind="requiredAccess: 'DatabaseReadWrite'"
                                                title="Delete the index"><i className="icon-trash"/></button>
                                    </div>
                                </div>
                            </div>
                        </div>
                    </div>
                </div>
            </div>
            <div className="sidebyside-actions" data-bind="with: replacement, visible: replacement">
                <div className="panel panel-state panel-warning">
                    {index.nodesInfo.map(nodeInfo => (
                        <div key={indexNodeInfoKey(nodeInfo)}>
                            <span className="margin-right">Shard #{nodeInfo.location.shardNumber}</span>
                            <span className="margin-right">Node Tag: {nodeInfo.location.nodeTag}</span>
                            { nodeInfo.status === "loaded" && (
                                <>
                                    <span className="margin-right">Errors: {nodeInfo.details.errorCount}</span>
                                    <span>Entries: {nodeInfo.details.entriesCount}</span>
                                </>
                            )}
                        </div>
                    ))}
                </div>
            </div>
        </div>
    )
}



function badgeClass(index: IndexSharedInfo) {
    if (IndexUtils.isFaulty(index)) {
        return "state-danger";
    }

    if (IndexUtils.isErrorState(index)) {
        return "state-danger";
    }

    if (IndexUtils.isPausedState(index)) {
        return "state-warnwing";
    }

    if (IndexUtils.isDisabledState(index)) {
        return "state-warning";
    }

    if (IndexUtils.isIdleState(index)) {
        return "state-warning";
    }

    if (IndexUtils.isErrorState(index)) {
        return "state-danger";
    }

    return "state-success";
}

function badgeText(index: IndexSharedInfo) {
    if (IndexUtils.isFaulty(index)) {
        return "Faulty";
    }

    if (IndexUtils.isErrorState(index)) {
        return "Error";
    }

    if (IndexUtils.isPausedState(index)) {
        return "Paused";
    }

    if (IndexUtils.isDisabledState(index)) {
        return "Disabled";
    }

    if (IndexUtils.isIdleState(index)) {
        return "Idle";
    }

    return "Normal";
}

const indexUniqueId = (index: IndexSharedInfo) => "index_" + index.name;

const indexNodeInfoKey = (nodeInfo: IndexNodeInfo) => "$" + nodeInfo.location.shardNumber + "@" + nodeInfo.location.nodeTag;

