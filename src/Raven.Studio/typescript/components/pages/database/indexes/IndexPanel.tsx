import React, { ForwardedRef, forwardRef, memo, MouseEvent, useCallback, useMemo, useRef, useState } from "react";
import classNames from "classnames";
import IndexPriority = Raven.Client.Documents.Indexes.IndexPriority;
import { IndexNodeInfo, IndexNodeInfoDetails, IndexSharedInfo } from "../../../models/indexes";
import IndexLockMode = Raven.Client.Documents.Indexes.IndexLockMode;
import { useAppUrls } from "../../../hooks/useAppUrls";
import IndexUtils from "../../../utils/IndexUtils";
import { useEventsCollector } from "../../../hooks/useEventsCollector";
import { withPreventDefault } from "../../../utils/common";
import indexStalenessReasons from "viewmodels/database/indexes/indexStalenessReasons";
import database = require("models/resources/database");
import app from "durandal/app";
import { IndexProgress } from "./IndexProgress";
import { useAccessManager } from "../../../hooks/useAccessManager";
import IndexRunningStatus = Raven.Client.Documents.Indexes.IndexRunningStatus;
import { UncontrolledTooltip } from "../../../common/UncontrolledTooltip";

interface IndexPanelProps {
    database: database;
    index: IndexSharedInfo;
    globalIndexingStatus: IndexRunningStatus;
    setPriority: (priority: IndexPriority) => Promise<void>;
    setLockMode: (lockMode: IndexLockMode) => Promise<void>;
    enableIndexing: () => Promise<void>;
    disableIndexing: () => Promise<void>;
    pauseIndexing: () => Promise<void>;
    resumeIndexing: () => Promise<void>;
    deleteIndex: () => Promise<void>;
    resetIndex: () => Promise<void>;
    openFaulty: (location: databaseLocationSpecifier) => Promise<void>;
    selected: boolean;
    hasReplacement?: boolean;
    toggleSelection: () => void;
    ref?: any;
}

export const IndexPanel = forwardRef(IndexPanelInternal);

export function IndexPanelInternal(props: IndexPanelProps, ref: ForwardedRef<HTMLDivElement>) {
    const { index, selected, toggleSelection, database, hasReplacement, globalIndexingStatus } = props;

    const { canReadWriteDatabase, canReadOnlyDatabase } = useAccessManager();

    const isReplacement = IndexUtils.isSideBySide(index);

    const eventsCollector = useEventsCollector();

    const [updatingLocalPriority, setUpdatingLocalPriority] = useState(false);
    const [updatingLockMode, setUpdatingLockMode] = useState(false);
    const [updatingState, setUpdatingState] = useState(false); //TODO: bind me!

    const setPriority = async (e: MouseEvent, priority: IndexPriority) => {
        e.preventDefault();
        if (priority !== index.priority) {
            setUpdatingLocalPriority(true);
            try {
                await props.setPriority(priority);
            } finally {
                setUpdatingLocalPriority(false);
            }
        }
    };

    const setLockMode = async (e: MouseEvent, lockMode: IndexLockMode) => {
        e.preventDefault();
        if (lockMode !== index.lockMode) {
            setUpdatingLockMode(true);
            try {
                await props.setLockMode(lockMode);
            } finally {
                setUpdatingLockMode(false);
            }
        }
    };

    const enableIndexing = async (e: MouseEvent) => {
        e.preventDefault();
        eventsCollector.reportEvent("indexes", "set-state", "enabled");
        setUpdatingState(true);
        try {
            await props.enableIndexing();
        } finally {
            setUpdatingState(false);
        }
    };

    const disableIndexing = async (e: MouseEvent) => {
        e.preventDefault();
        eventsCollector.reportEvent("indexes", "set-state", "disabled");
        setUpdatingState(true);
        try {
            await props.disableIndexing();
        } finally {
            setUpdatingState(false);
        }
    };

    const pauseIndexing = async (e: MouseEvent) => {
        e.preventDefault();
        eventsCollector.reportEvent("indexes", "pause");
        setUpdatingState(true);
        try {
            await props.pauseIndexing();
        } finally {
            setUpdatingState(false);
        }
    };

    const resumeIndexing = async (e: MouseEvent) => {
        e.preventDefault();
        eventsCollector.reportEvent("indexes", "pause");
        setUpdatingState(true);
        try {
            await props.resumeIndexing();
        } finally {
            setUpdatingState(false);
        }
    };

    const deleteIndex = async (e: MouseEvent) => {
        e.preventDefault();
        return props.deleteIndex();
    };

    const showStaleReasons = (index: IndexSharedInfo, location: databaseLocationSpecifier) => {
        const view = new indexStalenessReasons(database, index.name, location);
        eventsCollector.reportEvent("indexes", "show-stale-reasons");
        app.showBootstrapDialog(view);
    };

    const openFaulty = async (location: databaseLocationSpecifier) => {
        await props.openFaulty(location);
    };

    const resetIndex = () => props.resetIndex();

    const { forCurrentDatabase: urls } = useAppUrls();
    const queryUrl = urls.query(index.name)();
    const termsUrl = urls.terms(index.name)();
    const editUrl = urls.editIndex(index.name)();

    const [reduceOutputId] = useState(() => _.uniqueId("reduce-output-id"));

    return (
        <div className={classNames({ "sidebyside-indexes": hasReplacement })}>
            <div
                className={classNames("panel panel-state panel-hover index", { "has-replacement": hasReplacement })}
                ref={ref}
            >
                <div className="padding padding-sm js-index-template" id={indexUniqueId(index)}>
                    <div className="row">
                        <div className="col-xs-12 col-sm-6 col-xl-4 info-container">
                            <div className="flex-horizontal">
                                {canReadWriteDatabase(database) && (
                                    <div className="checkbox">
                                        <input
                                            type="checkbox"
                                            className="styled"
                                            checked={selected}
                                            onChange={toggleSelection}
                                        />
                                        <label />
                                    </div>
                                )}

                                <h3 className="index-name flex-grow">
                                    <a href={editUrl} title={index.name}>
                                        {index.name}
                                    </a>
                                </h3>
                                {index.sourceType === "Counters" && (
                                    <i className="icon-new-counter" title="Index source: Counters" />
                                )}
                                {index.sourceType === "TimeSeries" && (
                                    <i className="icon-timeseries" title="Index source: Time Series" />
                                )}
                                {index.sourceType === "Documents" && (
                                    <i className="icon-documents" title="Index source: Documents" />
                                )}
                            </div>
                            <div className="flex-horizontal clear-left index-info nospacing">
                                <div className="index-type-icon" id={reduceOutputId}>
                                    {index.reduceOutputCollectionName &&
                                        !index.patternForReferencesToReduceOutputCollection && (
                                            <span>
                                                <i className="icon-output-collection" />
                                            </span>
                                        )}
                                    {index.patternForReferencesToReduceOutputCollection && (
                                        <span>
                                            <i className="icon-reference-pattern" />
                                        </span>
                                    )}
                                </div>
                                <UncontrolledTooltip target={reduceOutputId} animation placement="right">
                                    <>
                                        {index.reduceOutputCollectionName && (
                                            <span>
                                                Reduce Results are saved in Collection:
                                                <br />
                                                <strong>{index.reduceOutputCollectionName}</strong>
                                            </span>
                                        )}
                                        {index.collectionNameForReferenceDocuments && (
                                            <span>
                                                <br />
                                                Referencing Documents are saved in Collection:
                                                <br />
                                                <strong>{index.collectionNameForReferenceDocuments}</strong>
                                            </span>
                                        )}
                                        {!index.collectionNameForReferenceDocuments &&
                                            index.patternForReferencesToReduceOutputCollection && (
                                                <span>
                                                    <br />
                                                    Referencing Documents are saved in Collection:
                                                    <br />
                                                    <strong>{index.reduceOutputCollectionName}/References</strong>
                                                </span>
                                            )}
                                    </>
                                </UncontrolledTooltip>
                                <div className="index-type">
                                    <span>{IndexUtils.formatType(index.type)}</span>
                                    {hasReplacement && (
                                        <span className="margin-left margin-left-sm">
                                            <span className="label label-warning">OLD</span>
                                        </span>
                                    )}
                                    {isReplacement && (
                                        <span className="margin-left margin-left-sm">
                                            <span className="label label-warning">NEW</span>
                                        </span>
                                    )}
                                </div>
                            </div>
                        </div>
                        {!IndexUtils.isFaulty(index) && (
                            <div className="col-xs-12 col-sm-12 col-xl-5 vertical-divider properties-container">
                                {!IndexUtils.isSideBySide(index) && (
                                    <div className="properties-item priority">
                                        <span className="properties-label">Priority:</span>
                                        <div className="btn-group properties-value">
                                            <button
                                                type="button"
                                                className={classNames("btn set-size dropdown-toggle", {
                                                    "btn-spinner": updatingLocalPriority,
                                                    enable: !updatingLocalPriority,
                                                })}
                                                data-toggle="dropdown"
                                                disabled={!canReadWriteDatabase(database)}
                                            >
                                                {index.priority === "Normal" && (
                                                    <span>
                                                        <i className="icon-check" />
                                                        <span>Normal</span>
                                                    </span>
                                                )}
                                                {index.priority === "Low" && (
                                                    <span>
                                                        <i className="icon-coffee" />
                                                        <span>Low</span>
                                                    </span>
                                                )}
                                                {index.priority === "High" && (
                                                    <span>
                                                        <i className="icon-force" />
                                                        <span>High</span>
                                                    </span>
                                                )}
                                                <span className="caret" />
                                            </button>
                                            <ul className="dropdown-menu">
                                                <li>
                                                    <a href="#" onClick={(e) => setPriority(e, "Low")} title="Low">
                                                        <i className="icon-coffee" />
                                                        <span>Low</span>
                                                    </a>
                                                </li>
                                                <li>
                                                    <a
                                                        href="#"
                                                        onClick={(e) => setPriority(e, "Normal")}
                                                        title="Normal"
                                                    >
                                                        <i className="icon-check" />
                                                        <span>Normal</span>
                                                    </a>
                                                </li>
                                                <li>
                                                    <a href="#" onClick={(e) => setPriority(e, "High")} title="High">
                                                        <i className="icon-force" />
                                                        <span>High</span>
                                                    </a>
                                                </li>
                                            </ul>
                                        </div>
                                    </div>
                                )}

                                {index.type !== "AutoMap" &&
                                    index.type !== "AutoMapReduce" &&
                                    !IndexUtils.isSideBySide(index) && (
                                        <div className="properties-item mode">
                                            <span className="properties-label">Mode:</span>
                                            <div className="btn-group properties-value">
                                                <button
                                                    type="button"
                                                    className={classNames("btn set-size dropdown-toggle", {
                                                        "btn-spinner": updatingLockMode,
                                                        enable: !updatingLockMode,
                                                    })}
                                                    data-toggle="dropdown"
                                                    disabled={!canReadWriteDatabase(database)}
                                                >
                                                    {index.lockMode === "Unlock" && (
                                                        <span>
                                                            <i className="icon-unlock" />
                                                            <span>Unlocked</span>
                                                        </span>
                                                    )}
                                                    {index.lockMode === "LockedIgnore" && (
                                                        <span>
                                                            <i className="icon-lock" />
                                                            <span>Locked</span>
                                                        </span>
                                                    )}
                                                    {index.lockMode === "LockedError" && (
                                                        <span>
                                                            <i className="icon-lock-error" />
                                                            <span>Locked (Error)</span>
                                                        </span>
                                                    )}
                                                    <span className="caret" />
                                                </button>
                                                <ul className="dropdown-menu">
                                                    <li>
                                                        <a
                                                            href="#"
                                                            onClick={(e) => setLockMode(e, "Unlock")}
                                                            title="Unlocked: The index is unlocked for changes; apps can modify it, e.g. via IndexCreation.CreateIndexes()."
                                                        >
                                                            <i className="icon-unlock" />
                                                            <span>Unlock</span>
                                                        </a>
                                                    </li>
                                                    <li className="divider" />
                                                    <li>
                                                        <a
                                                            href="#"
                                                            onClick={(e) => setLockMode(e, "LockedIgnore")}
                                                            title="Locked: The index is locked for changes; apps cannot modify it. Programmatic attempts to modify the index will be ignored."
                                                        >
                                                            <i className="icon-lock" />
                                                            <span>Lock</span>
                                                        </a>
                                                    </li>
                                                    <li>
                                                        <a
                                                            href="#"
                                                            onClick={(e) => setLockMode(e, "LockedError")}
                                                            title="Locked + Error: The index is locked for changes; apps cannot modify it. An error will be thrown if an app attempts to modify it."
                                                        >
                                                            <i className="icon-lock-error" />
                                                            <span>Lock (Error)</span>
                                                        </a>
                                                    </li>
                                                </ul>
                                            </div>
                                        </div>
                                    )}
                            </div>
                        )}

                        <div className="col-xs-12 col-sm-6 col-xl-3 actions-container">
                            <div className="actions">
                                <div className="btn-toolbar pull-right-sm" role="toolbar">
                                    <div className="btn-group properties-value">
                                        <button
                                            type="button"
                                            className="btn btn-default"
                                            data-toggle="dropdown"
                                            data-bind="css: { 'btn-spinner': _.includes($root.spinners.localState(), name) },
                                           enable: $root.globalIndexingStatus() === 'Running'  && !_.includes($root.spinners.localState(), name),
                                           requiredAccess: 'DatabaseReadWrite', requiredAccessOptions: { strategy: 'disable' }"
                                        >
                                            Set State
                                            <span className="caret" />
                                        </button>
                                        <ul className="dropdown-menu">
                                            <li data-bind="visible: canBeEnabled()">
                                                <a
                                                    href="#"
                                                    onClick={enableIndexing}
                                                    title="Enable indexing on ALL cluster nodes"
                                                >
                                                    <i className="icon-play" />
                                                    <span>Enable indexing</span>
                                                </a>
                                            </li>
                                            <li data-bind="visible: canBeDisabled()">
                                                <a
                                                    href="#"
                                                    onClick={disableIndexing}
                                                    title="Disable indexing on ALL cluster nodes"
                                                >
                                                    <i className="icon-cancel" />
                                                    <span>Disable indexing</span>
                                                </a>
                                            </li>
                                            <li data-bind="visible: canBePaused()">
                                                <a
                                                    href="#"
                                                    onClick={pauseIndexing}
                                                    className="text-warning"
                                                    title="Pause until restart"
                                                >
                                                    <i className="icon-pause" />
                                                    <span>Pause indexing until restart</span>
                                                </a>
                                            </li>
                                            <li data-bind="visible: canBeResumed()">
                                                <a
                                                    href="#"
                                                    onClick={resumeIndexing}
                                                    className="text-success"
                                                    title="Resume indexing"
                                                >
                                                    <i className="icon-play" />
                                                    <span>Resume indexing</span>
                                                </a>
                                            </li>
                                        </ul>
                                    </div>

                                    {!IndexUtils.isFaulty(index) && (
                                        <div className="btn-group" role="group">
                                            <a className="btn btn-default" href={queryUrl}>
                                                <i className="icon-search" />
                                                <span>Query</span>
                                            </a>
                                            <button
                                                type="button"
                                                className="btn btn-default dropdown-toggle"
                                                data-toggle="dropdown"
                                                aria-haspopup="true"
                                                aria-expanded="false"
                                            >
                                                <span className="caret" />
                                                <span className="sr-only">Toggle Dropdown</span>
                                            </button>
                                            <ul className="dropdown-menu">
                                                <li>
                                                    <a href={termsUrl}>
                                                        <i className="icon-terms" /> Terms
                                                    </a>
                                                </li>
                                            </ul>
                                        </div>
                                    )}

                                    <div className="btn-group" role="group">
                                        {!IndexUtils.isAutoIndex(index) && !canReadOnlyDatabase(database) && (
                                            <a className="btn btn-default" href={editUrl} title="Edit index">
                                                <i className="icon-edit" />
                                            </a>
                                        )}
                                        {(IndexUtils.isAutoIndex(index) || canReadOnlyDatabase(database)) && (
                                            <a className="btn btn-default" href={editUrl} title="View index">
                                                <i className="icon-preview" />
                                            </a>
                                        )}
                                    </div>
                                    {canReadWriteDatabase(database) && (
                                        <div className="btn-group" role="group">
                                            <button
                                                className="btn btn-warning"
                                                type="button"
                                                onClick={resetIndex}
                                                title="Reset index (rebuild)"
                                            >
                                                <i className="icon-reset-index" />
                                            </button>
                                            <button
                                                className="btn btn-danger"
                                                onClick={deleteIndex}
                                                title="Delete the index"
                                            >
                                                <i className="icon-trash" />
                                            </button>
                                        </div>
                                    )}
                                </div>
                            </div>
                        </div>
                    </div>
                </div>
            </div>
            <div>
                <div className="panel panel-state panel-info">
                    {index.nodesInfo.map((nodeInfo) => (
                        <div key={indexNodeInfoKey(nodeInfo)}>
                            <span className="margin-right">Shard #{nodeInfo.location.shardNumber}</span>
                            <span className="margin-right">Node Tag: {nodeInfo.location.nodeTag}</span>
                            {nodeInfo.status === "loaded" && (
                                <>
                                    <span className="margin-right">Errors: {nodeInfo.details.errorCount}</span>
                                    <span className="margin-right">Entries: {nodeInfo.details.entriesCount}</span>
                                    <span
                                        className={classNames(
                                            "badge margin-right",
                                            badgeClass(index, nodeInfo.details, globalIndexingStatus)
                                        )}
                                    >
                                        {badgeText(index, nodeInfo.details, globalIndexingStatus)}
                                    </span>
                                    {nodeInfo.details.stale ? (
                                        <span className="set-size">
                                            <a
                                                title="Show stale reason"
                                                href="#"
                                                className="text-warning"
                                                onClick={withPreventDefault(() =>
                                                    showStaleReasons(index, nodeInfo.location)
                                                )}
                                            >
                                                <span>Stale</span>&nbsp;&nbsp;
                                                <i className="icon-help" />
                                            </a>
                                        </span>
                                    ) : (
                                        <span className="set-size">
                                            <i className="icon-check" />
                                            <span>Up to date</span>
                                        </span>
                                    )}
                                    {nodeInfo.details.faulty ? (
                                        <button
                                            type="button"
                                            className="btn btn-default"
                                            onClick={() => openFaulty(nodeInfo.location)}
                                            title="Open index"
                                        >
                                            <i className="icon-arrow-filled-up" />
                                        </button>
                                    ) : (
                                        <IndexProgress progress={nodeInfo.progress} nodeDetails={nodeInfo.details} />
                                    )}
                                </>
                            )}
                        </div>
                    ))}
                </div>
            </div>
        </div>
    );
}

function badgeClass(index: IndexSharedInfo, details: IndexNodeInfoDetails, globalIndexingStatus: IndexRunningStatus) {
    if (IndexUtils.isFaulty(index)) {
        return "badge-danger";
    }

    if (IndexUtils.isErrorState(details)) {
        return "badge-danger";
    }

    if (IndexUtils.isPausedState(details, globalIndexingStatus)) {
        return "badge-warnwing";
    }

    if (IndexUtils.isDisabledState(details, globalIndexingStatus)) {
        return "badge-warning";
    }

    if (IndexUtils.isIdleState(details, globalIndexingStatus)) {
        return "badge-warning";
    }

    if (IndexUtils.isErrorState(details)) {
        return "badge-danger";
    }

    return "badge-success";
}

function badgeText(index: IndexSharedInfo, details: IndexNodeInfoDetails, globalIndexingStatus: IndexRunningStatus) {
    if (IndexUtils.isFaulty(index)) {
        return "Faulty";
    }

    if (IndexUtils.isErrorState(details)) {
        return "Error";
    }

    if (IndexUtils.isPausedState(details, globalIndexingStatus)) {
        return "Paused";
    }

    if (IndexUtils.isDisabledState(details, globalIndexingStatus)) {
        return "Disabled";
    }

    if (IndexUtils.isIdleState(details, globalIndexingStatus)) {
        return "Idle";
    }

    return "Normal";
}

const indexUniqueId = (index: IndexSharedInfo) => "index_" + index.name;

const indexNodeInfoKey = (nodeInfo: IndexNodeInfo) =>
    "$" + nodeInfo.location.shardNumber + "@" + nodeInfo.location.nodeTag;
