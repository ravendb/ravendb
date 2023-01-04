import React, { ForwardedRef, forwardRef, MouseEvent, useState } from "react";
import classNames from "classnames";
import IndexPriority = Raven.Client.Documents.Indexes.IndexPriority;
import { IndexSharedInfo } from "components/models/indexes";
import IndexLockMode = Raven.Client.Documents.Indexes.IndexLockMode;
import { useAppUrls } from "hooks/useAppUrls";
import IndexUtils from "../../../../utils/IndexUtils";
import { useEventsCollector } from "hooks/useEventsCollector";
import indexStalenessReasons from "viewmodels/database/indexes/indexStalenessReasons";
import database = require("models/resources/database");
import app from "durandal/app";
import { useAccessManager } from "hooks/useAccessManager";
import IndexRunningStatus = Raven.Client.Documents.Indexes.IndexRunningStatus;
import { UncontrolledTooltip } from "components/common/UncontrolledTooltip";
import { IndexDistribution, IndexProgress, JoinedIndexProgress } from "./IndexDistribution";
import IndexSourceType = Raven.Client.Documents.Indexes.IndexSourceType;
import {
    RichPanel,
    RichPanelActions,
    RichPanelDetailItem,
    RichPanelDetails,
    RichPanelHeader,
    RichPanelInfo,
    RichPanelName,
    RichPanelSelect,
} from "components/common/RichPanel";
import {
    Badge,
    Button,
    ButtonGroup,
    Collapse,
    DropdownItem,
    DropdownMenu,
    DropdownToggle,
    Input,
    Spinner,
    UncontrolledDropdown,
} from "reactstrap";
import assertUnreachable from "../../../../utils/assertUnreachable";
import useId from "hooks/useId";
import useBoolean from "hooks/useBoolean";

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

function getPriorityColor(index: IndexSharedInfo) {
    switch (index.priority) {
        case "Normal":
            return "light";
        case "High":
            return "warning";
        case "Low":
            return "info";
        default:
            assertUnreachable(index.priority);
    }
}

function getLockColor(index: IndexSharedInfo) {
    switch (index.lockMode) {
        case "LockedIgnore":
            return "warning";
        case "LockedError":
            return "warning";
        case "Unlock":
            return "light";
        default:
            assertUnreachable(index.lockMode);
    }
}

export function IndexPanelInternal(props: IndexPanelProps, ref: ForwardedRef<HTMLDivElement>) {
    const { index, selected, toggleSelection, database, hasReplacement, globalIndexingStatus } = props;

    const { canReadWriteDatabase, canReadOnlyDatabase } = useAccessManager();

    const { value: panelCollapsed, toggle: togglePanelCollapsed } = useBoolean(true);

    const isReplacement = IndexUtils.isSideBySide(index);
    const isFaulty = IndexUtils.hasAnyFaultyNode(index);

    const eventsCollector = useEventsCollector();

    const [updatingLocalPriority, setUpdatingLocalPriority] = useState(false);
    const [updatingLockMode, setUpdatingLockMode] = useState(false);
    const [updatingState, setUpdatingState] = useState(false);

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

    const priorityButtonColor = getPriorityColor(index);
    const lockButtonColor = getLockColor(index);

    const reduceOutputId = useId("reduce-output-id");

    return (
        <>
            <RichPanel className={classNames({ "index-sidebyside": hasReplacement || isReplacement })} innerRef={ref}>
                <RichPanelHeader id={indexUniqueId(index)}>
                    <RichPanelInfo>
                        <RichPanelSelect>
                            {canReadWriteDatabase(database) && (
                                <Input type="checkbox" onChange={toggleSelection} checked={selected} />
                            )}
                        </RichPanelSelect>

                        <RichPanelName>
                            <a href={editUrl} title={index.name}>
                                {index.name}
                            </a>
                        </RichPanelName>
                    </RichPanelInfo>
                    <RichPanelActions>
                        {!IndexUtils.hasAnyFaultyNode(index) && (
                            <>
                                {!IndexUtils.isSideBySide(index) && (
                                    <UncontrolledDropdown className="me-2">
                                        <DropdownToggle
                                            outline
                                            color={priorityButtonColor}
                                            disabled={!canReadWriteDatabase(database)}
                                        >
                                            {updatingLocalPriority && <Spinner size="sm" className="me-2" />}
                                            {!updatingLocalPriority && index.priority === "Normal" && (
                                                <span>
                                                    <i className="icon-check" />
                                                    <span>Normal Priority</span>
                                                </span>
                                            )}
                                            {!updatingLocalPriority && index.priority === "Low" && (
                                                <span>
                                                    <i className="icon-coffee" />
                                                    <span>Low Priority</span>
                                                </span>
                                            )}
                                            {!updatingLocalPriority && index.priority === "High" && (
                                                <span>
                                                    <i className="icon-force" />
                                                    <span>High Priority</span>
                                                </span>
                                            )}
                                        </DropdownToggle>

                                        <DropdownMenu>
                                            <DropdownItem onClick={(e) => setPriority(e, "Low")} title="Low">
                                                <i className="icon-coffee" /> <span>Low Priority</span>
                                            </DropdownItem>
                                            <DropdownItem onClick={(e) => setPriority(e, "Normal")} title="Normal">
                                                <i className="icon-check" /> <span>Normal Priority</span>
                                            </DropdownItem>
                                            <DropdownItem onClick={(e) => setPriority(e, "High")} title="High">
                                                <i className="icon-force" /> <span>High Priority</span>
                                            </DropdownItem>
                                        </DropdownMenu>
                                    </UncontrolledDropdown>
                                )}

                                {index.type !== "AutoMap" &&
                                    index.type !== "AutoMapReduce" &&
                                    !IndexUtils.isSideBySide(index) && (
                                        <UncontrolledDropdown className="me-3">
                                            <DropdownToggle
                                                outline
                                                color={lockButtonColor}
                                                disabled={!canReadWriteDatabase(database)}
                                            >
                                                {updatingLockMode && <Spinner size="sm" className="me-2" />}
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
                                            </DropdownToggle>

                                            <DropdownMenu>
                                                <DropdownItem
                                                    onClick={(e) => setLockMode(e, "Unlock")}
                                                    title="Unlocked: The index is unlocked for changes; apps can modify it, e.g. via IndexCreation.CreateIndexes()."
                                                >
                                                    <i className="icon-unlock" /> <span>Unlock</span>
                                                </DropdownItem>
                                                <DropdownItem divider />
                                                <DropdownItem
                                                    onClick={(e) => setLockMode(e, "LockedIgnore")}
                                                    title="Locked: The index is locked for changes; apps cannot modify it. Programmatic attempts to modify the index will be ignored."
                                                >
                                                    <i className="icon-lock" /> <span>Lock</span>
                                                </DropdownItem>
                                                <DropdownItem
                                                    onClick={(e) => setLockMode(e, "LockedError")}
                                                    title="Locked + Error: The index is locked for changes; apps cannot modify it. An error will be thrown if an app attempts to modify it."
                                                >
                                                    <i className="icon-lock-error" /> <span>Lock (Error)</span>
                                                </DropdownItem>
                                            </DropdownMenu>
                                        </UncontrolledDropdown>
                                    )}
                            </>
                        )}

                        {!IndexUtils.hasAnyFaultyNode(index) && (
                            <UncontrolledDropdown className="me-1">
                                <DropdownToggle
                                    data-bind="css: { 'btn-spinner': _.includes($root.spinners.localState(), name) },
                                           enable: $root.globalIndexingStatus() === 'Running'  && !_.includes($root.spinners.localState(), name),
                                           requiredAccess: 'DatabaseReadWrite', requiredAccessOptions: { strategy: 'disable' }"
                                >
                                    {updatingState && <Spinner size="sm" className="me-2" />}
                                    <span>Set State</span>
                                </DropdownToggle>

                                <DropdownMenu>
                                    <DropdownItem onClick={enableIndexing} title="Enable indexing">
                                        <i className="icon-play" /> <span>Enable indexing</span>
                                    </DropdownItem>
                                    <DropdownItem onClick={disableIndexing} title="Disable indexing">
                                        <i className="icon-cancel text-danger" /> <span>Disable indexing</span>
                                    </DropdownItem>
                                    <DropdownItem divider />
                                    <DropdownItem onClick={resumeIndexing} title="Resume indexing">
                                        <i className="icon-play" /> <span>Resume indexing</span>
                                    </DropdownItem>
                                    <DropdownItem onClick={pauseIndexing} title="Pause until restart">
                                        <i className="icon-pause text-warning" />{" "}
                                        <span>Pause indexing until restart</span>
                                    </DropdownItem>
                                </DropdownMenu>
                            </UncontrolledDropdown>
                        )}

                        {!IndexUtils.hasAnyFaultyNode(index) && (
                            <ButtonGroup className="me-1">
                                <Button variant="secondary" href={queryUrl}>
                                    <i className="icon-search" />
                                    <span>Query</span>
                                </Button>

                                <UncontrolledDropdown>
                                    <DropdownToggle className="dropdown-toggle" />

                                    <DropdownMenu end>
                                        <DropdownItem href={termsUrl}>
                                            {" "}
                                            <i className="icon-terms" /> Terms{" "}
                                        </DropdownItem>
                                    </DropdownMenu>
                                </UncontrolledDropdown>
                            </ButtonGroup>
                        )}

                        <ButtonGroup className="me-1">
                            {!IndexUtils.isAutoIndex(index) && !canReadOnlyDatabase(database) && (
                                <Button href={editUrl} title="Edit index">
                                    <i className="icon-edit" />
                                </Button>
                            )}
                            {(IndexUtils.isAutoIndex(index) || canReadOnlyDatabase(database)) && (
                                <Button href={editUrl} title="View index">
                                    <i className="icon-preview" />
                                </Button>
                            )}
                        </ButtonGroup>

                        {isFaulty && (
                            <Button onClick={() => openFaulty(index.nodesInfo[0].location)} className="me-1">
                                Open faulty index
                            </Button>
                        )}

                        {canReadWriteDatabase(database) && (
                            <ButtonGroup className="me-1">
                                <Button color="warning" onClick={resetIndex} title="Reset index (rebuild)">
                                    <i className="icon-reset-index" />
                                </Button>
                                <Button color="danger" onClick={deleteIndex} title="Delete the index">
                                    <i className="icon-trash" />
                                </Button>
                            </ButtonGroup>
                        )}
                        <Button color="secondary" onClick={togglePanelCollapsed} title="Toggle distribution details">
                            <i className={panelCollapsed ? "icon-expand-vertical" : "icon-collapse-vertical"} />
                        </Button>
                    </RichPanelActions>
                </RichPanelHeader>
                <RichPanelDetails>
                    {(index.reduceOutputCollectionName || index.patternForReferencesToReduceOutputCollection) && (
                        <RichPanelDetailItem>
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
                            </div>
                        </RichPanelDetailItem>
                    )}

                    {(hasReplacement || isReplacement) && (
                        <RichPanelDetailItem>
                            {hasReplacement && (
                                <Badge pill color="warning" className="ms-3">
                                    OLD
                                </Badge>
                            )}
                            {isReplacement && (
                                <Badge pill color="warning" className="ms-3">
                                    NEW
                                </Badge>
                            )}
                        </RichPanelDetailItem>
                    )}
                    <RichPanelDetailItem className={isFaulty ? "text-danger" : ""}>
                        <i className={IndexUtils.indexTypeIcon(index.type)} />
                        {IndexUtils.formatType(index.type)}
                    </RichPanelDetailItem>
                    <IndexSourceTypeComponent sourceType={index.sourceType} />
                    <RichPanelDetailItem>
                        <i className="icon-search" />
                        {index.searchEngine}
                    </RichPanelDetailItem>

                    {!isFaulty && <InlineDetails index={index} />}
                </RichPanelDetails>
                <Collapse isOpen={!panelCollapsed}>
                    <IndexDistribution
                        index={index}
                        globalIndexingStatus={globalIndexingStatus}
                        showStaleReason={(location) => showStaleReasons(index, location)}
                        openFaulty={openFaulty}
                    />
                </Collapse>
            </RichPanel>
        </>
    );
}

function IndexSourceTypeComponent(props: { sourceType: IndexSourceType }) {
    const { sourceType } = props;

    return (
        <RichPanelDetailItem>
            {sourceType === "Counters" && (
                <>
                    <i className="icon-new-counter" title="Index source: Counters" />
                    Counters
                </>
            )}
            {sourceType === "TimeSeries" && (
                <>
                    <i className="icon-timeseries" title="Index source: Time Series" />
                    Time Series
                </>
            )}
            {sourceType === "Documents" && (
                <>
                    <i className="icon-documents" title="Index source: Documents" />
                    Documents
                </>
            )}
        </RichPanelDetailItem>
    );
}

interface InlineDetailsProps {
    index: IndexSharedInfo;
}

function InlineDetails(props: InlineDetailsProps) {
    const { index } = props;

    const estimatedEntries = IndexUtils.estimateEntriesCount(index)?.toLocaleString() ?? "-";
    const errorsCount = index.nodesInfo.filter((x) => x.details).reduce((prev, x) => prev + x.details.errorCount, 0);

    return (
        <>
            <RichPanelDetailItem>
                <i className="icon-list" />
                Entries
                <div className="value">{estimatedEntries}</div>
            </RichPanelDetailItem>
            <RichPanelDetailItem
                className={classNames("index-detail-item", {
                    "text-danger": errorsCount > 0,
                })}
            >
                <i className="icon-warning" />
                Errors
                <div className="value">{errorsCount.toLocaleString()}</div>
            </RichPanelDetailItem>
            <RichPanelDetailItem>
                <JoinedIndexProgress index={index} />
            </RichPanelDetailItem>
        </>
    );
}

const indexUniqueId = (index: IndexSharedInfo) => "index_" + index.name;
