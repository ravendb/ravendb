import React, { ForwardedRef, forwardRef, MouseEvent, useState } from "react";
import classNames from "classnames";
import IndexPriority = Raven.Client.Documents.Indexes.IndexPriority;
import IndexLockMode = Raven.Client.Documents.Indexes.IndexLockMode;
import IndexRunningStatus = Raven.Client.Documents.Indexes.IndexRunningStatus;
import IndexSourceType = Raven.Client.Documents.Indexes.IndexSourceType;
import { IndexSharedInfo } from "components/models/indexes";
import { useAppUrls } from "hooks/useAppUrls";
import IndexUtils from "../../../../utils/IndexUtils";
import { useEventsCollector } from "hooks/useEventsCollector";
import indexStalenessReasons from "viewmodels/database/indexes/indexStalenessReasons";
import app from "durandal/app";
import { IndexDistribution, JoinedIndexProgress } from "./IndexDistribution";
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
    UncontrolledTooltip,
} from "reactstrap";
import assertUnreachable from "../../../../utils/assertUnreachable";
import useId from "hooks/useId";
import useBoolean from "hooks/useBoolean";
import { Icon } from "components/common/Icon";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { useAppSelector } from "components/store";
import { accessManagerSelectors } from "components/common/shell/accessManagerSliceSelectors";
import ResetIndexesButton from "components/pages/database/indexes/list/partials/ResetIndexesButton";

export interface IndexPanelProps {
    index: IndexSharedInfo;
    globalIndexingStatus: IndexRunningStatus;
    setPriority: (priority: IndexPriority) => Promise<void>;
    setLockMode: (lockMode: IndexLockMode) => Promise<void>;
    startIndexing: () => Promise<void>;
    disableIndexing: () => Promise<void>;
    pauseIndexing: () => Promise<void>;
    deleteIndex: () => Promise<void>;
    resetIndex: (mode?: Raven.Client.Documents.Indexes.IndexResetMode) => void;
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
            return "secondary";
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
            return "secondary";
        default:
            assertUnreachable(index.lockMode);
    }
}

export function IndexPanelInternal(props: IndexPanelProps, ref: ForwardedRef<HTMLDivElement>) {
    const { index, selected, toggleSelection, hasReplacement, globalIndexingStatus, resetIndex } = props;

    const hasDatabaseWriteAccess = useAppSelector(accessManagerSelectors.getHasDatabaseWriteAccess)();
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);

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

    const startIndexing = async (e: MouseEvent) => {
        e.preventDefault();
        eventsCollector.reportEvent("indexes", "set-state", "enabled");
        setUpdatingState(true);
        try {
            await props.startIndexing();
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

    const deleteIndex = async (e: MouseEvent) => {
        e.preventDefault();
        return props.deleteIndex();
    };

    const showStaleReasons = (index: IndexSharedInfo, location: databaseLocationSpecifier) => {
        const view = new indexStalenessReasons(databaseName, index.name, location);
        eventsCollector.reportEvent("indexes", "show-stale-reasons");
        app.showBootstrapDialog(view);
    };

    const openFaulty = async (location: databaseLocationSpecifier) => {
        await props.openFaulty(location);
    };

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
                            {hasDatabaseWriteAccess && (
                                <Input type="checkbox" onChange={toggleSelection} checked={selected} />
                            )}
                        </RichPanelSelect>
                        <div className="flex-grow">
                            <RichPanelName>
                                <a href={editUrl} title={index.name}>
                                    {index.name}
                                </a>
                            </RichPanelName>
                        </div>
                    </RichPanelInfo>
                    <RichPanelActions>
                        {!IndexUtils.hasAnyFaultyNode(index) && (
                            <>
                                {!IndexUtils.isSideBySide(index) && (
                                    <UncontrolledDropdown>
                                        <DropdownToggle
                                            outline
                                            color={priorityButtonColor}
                                            disabled={!hasDatabaseWriteAccess}
                                        >
                                            {updatingLocalPriority && <Spinner size="sm" className="me-2" />}
                                            {!updatingLocalPriority && index.priority === "Normal" && (
                                                <span>
                                                    <Icon icon="check" />
                                                    <span>Normal Priority</span>
                                                </span>
                                            )}
                                            {!updatingLocalPriority && index.priority === "Low" && (
                                                <span>
                                                    <Icon icon="coffee" />
                                                    <span>Low Priority</span>
                                                </span>
                                            )}
                                            {!updatingLocalPriority && index.priority === "High" && (
                                                <span>
                                                    <Icon icon="force" />
                                                    <span>High Priority</span>
                                                </span>
                                            )}
                                        </DropdownToggle>

                                        <DropdownMenu>
                                            <DropdownItem onClick={(e) => setPriority(e, "Low")} title="Low">
                                                <Icon icon="coffee" /> <span>Low Priority</span>
                                            </DropdownItem>
                                            <DropdownItem onClick={(e) => setPriority(e, "Normal")} title="Normal">
                                                <Icon icon="check" /> <span>Normal Priority</span>
                                            </DropdownItem>
                                            <DropdownItem onClick={(e) => setPriority(e, "High")} title="High">
                                                <Icon icon="force" /> <span>High Priority</span>
                                            </DropdownItem>
                                        </DropdownMenu>
                                    </UncontrolledDropdown>
                                )}

                                {index.type !== "AutoMap" &&
                                    index.type !== "AutoMapReduce" &&
                                    !IndexUtils.isSideBySide(index) && (
                                        <UncontrolledDropdown>
                                            <DropdownToggle
                                                outline
                                                color={lockButtonColor}
                                                disabled={!hasDatabaseWriteAccess}
                                            >
                                                {updatingLockMode && <Spinner size="sm" className="me-2" />}
                                                {index.lockMode === "Unlock" && (
                                                    <span>
                                                        <Icon icon="unlock" />
                                                        <span>Unlocked</span>
                                                    </span>
                                                )}
                                                {index.lockMode === "LockedIgnore" && (
                                                    <span>
                                                        <Icon icon="lock" />
                                                        <span>Locked</span>
                                                    </span>
                                                )}
                                                {index.lockMode === "LockedError" && (
                                                    <span>
                                                        <Icon icon="lock-error" />
                                                        <span>Locked (Error)</span>
                                                    </span>
                                                )}
                                            </DropdownToggle>

                                            <DropdownMenu>
                                                <DropdownItem
                                                    onClick={(e) => setLockMode(e, "Unlock")}
                                                    title="Unlocked: The index is unlocked for changes; apps can modify it, e.g. via IndexCreation.CreateIndexes()."
                                                >
                                                    <Icon icon="unlock" /> <span>Unlock</span>
                                                </DropdownItem>
                                                <DropdownItem divider />
                                                <DropdownItem
                                                    onClick={(e) => setLockMode(e, "LockedIgnore")}
                                                    title="Locked: The index is locked for changes; apps cannot modify it. Programmatic attempts to modify the index will be ignored."
                                                >
                                                    <Icon icon="lock" /> <span>Lock</span>
                                                </DropdownItem>
                                                <DropdownItem
                                                    onClick={(e) => setLockMode(e, "LockedError")}
                                                    title="Locked + Error: The index is locked for changes; apps cannot modify it. An error will be thrown if an app attempts to modify it."
                                                >
                                                    <Icon icon="lock-error" /> <span>Lock (Error)</span>
                                                </DropdownItem>
                                            </DropdownMenu>
                                        </UncontrolledDropdown>
                                    )}
                            </>
                        )}

                        {!IndexUtils.hasAnyFaultyNode(index) && (
                            <UncontrolledDropdown>
                                <DropdownToggle
                                    data-bind="css: { 'btn-spinner': _.includes($root.spinners.localState(), name) },
                                           enable: $root.globalIndexingStatus() === 'Running'  && !_.includes($root.spinners.localState(), name),
                                           requiredAccess: 'DatabaseReadWrite', requiredAccessOptions: { strategy: 'disable' }"
                                >
                                    {updatingState && <Spinner size="sm" className="me-2" />}
                                    <span>Set State</span>
                                </DropdownToggle>

                                <DropdownMenu>
                                    <DropdownItem onClick={startIndexing} title="Start indexing">
                                        <Icon icon="play" /> <span>Start indexing</span>
                                    </DropdownItem>
                                    <DropdownItem onClick={disableIndexing} title="Disable indexing">
                                        <Icon icon="stop" color="danger" /> <span>Disable indexing</span>
                                    </DropdownItem>
                                    <DropdownItem onClick={pauseIndexing} title="Pause until restart">
                                        <Icon icon="pause" color="warning" /> <span>Pause indexing until restart</span>
                                    </DropdownItem>
                                </DropdownMenu>
                            </UncontrolledDropdown>
                        )}

                        {!IndexUtils.hasAnyFaultyNode(index) && (
                            <UncontrolledDropdown group>
                                <Button variant="secondary" href={queryUrl}>
                                    <Icon icon="search" />
                                    <span>Query</span>
                                </Button>
                                <DropdownToggle className="dropdown-toggle" />

                                <DropdownMenu end>
                                    <DropdownItem href={termsUrl}>
                                        {" "}
                                        <Icon icon="terms" /> Terms{" "}
                                    </DropdownItem>
                                </DropdownMenu>
                            </UncontrolledDropdown>
                        )}

                        <ButtonGroup>
                            {!IndexUtils.isAutoIndex(index) && hasDatabaseWriteAccess && (
                                <Button href={editUrl} title="Edit index">
                                    <Icon icon="edit" margin="m-0" />
                                </Button>
                            )}
                            {(IndexUtils.isAutoIndex(index) || !hasDatabaseWriteAccess) && (
                                <Button href={editUrl} title="View index">
                                    <Icon icon="preview" margin="m-0" />
                                </Button>
                            )}
                        </ButtonGroup>

                        {isFaulty && (
                            <Button onClick={() => openFaulty(index.nodesInfo[0].location)}>Open faulty index</Button>
                        )}
                        {hasDatabaseWriteAccess && (
                            <>
                                <ResetIndexesButton resetIndex={resetIndex} isDropdownVisible={!isReplacement} />
                                <Button color="danger" onClick={deleteIndex} title="Delete the index">
                                    <Icon icon="trash" margin="m-0" />
                                </Button>
                            </>
                        )}
                    </RichPanelActions>
                </RichPanelHeader>
                <RichPanelDetails className="pb-1">
                    <RichPanelDetailItem>
                        <Button
                            onClick={togglePanelCollapsed}
                            title={panelCollapsed ? "Expand distribution details" : "Collapse distribution details"}
                            className="btn-toggle-panel rounded-pill"
                        >
                            <Icon icon={panelCollapsed ? "unfold" : "fold"} margin="m-0" />
                        </Button>
                    </RichPanelDetailItem>
                    {(index.reduceOutputCollectionName || index.patternForReferencesToReduceOutputCollection) && (
                        <RichPanelDetailItem>
                            <div className="index-type-icon" id={reduceOutputId}>
                                {index.reduceOutputCollectionName &&
                                    !index.patternForReferencesToReduceOutputCollection && (
                                        <span>
                                            <Icon icon="output-collection" margin="m-0" />
                                        </span>
                                    )}
                                {index.patternForReferencesToReduceOutputCollection && (
                                    <span>
                                        <Icon icon="reference-pattern" margin="m-0" />
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
                    <ReferencedCollections collections={index.referencedCollections} />
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
                        <Icon icon={IndexUtils.indexTypeIcon(index.type)} />
                        {IndexUtils.formatType(index.type)}
                    </RichPanelDetailItem>
                    <IndexSourceTypeComponent sourceType={index.sourceType} />
                    <RichPanelDetailItem>
                        <Icon icon="search" />
                        {index.searchEngine}
                    </RichPanelDetailItem>

                    {!isFaulty && (
                        <InlineDetails
                            index={index}
                            toggleLocationDetails={togglePanelCollapsed}
                            errorsUrl={urls.indexErrors()}
                        />
                    )}
                </RichPanelDetails>
                <div className="px-3 pb-2">
                    <Collapse isOpen={!panelCollapsed}>
                        <IndexDistribution
                            index={index}
                            globalIndexingStatus={globalIndexingStatus}
                            showStaleReason={(location) => showStaleReasons(index, location)}
                            openFaulty={openFaulty}
                        />
                    </Collapse>
                </div>
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
                    <Icon icon="new-counter" title="Index source: Counters" />
                    Counters
                </>
            )}
            {sourceType === "TimeSeries" && (
                <>
                    <Icon icon="timeseries" title="Index source: Time Series" />
                    Time Series
                </>
            )}
            {sourceType === "Documents" && (
                <>
                    <Icon icon="documents" title="Index source: Documents" />
                    Documents
                </>
            )}
        </RichPanelDetailItem>
    );
}

interface InlineDetailsProps {
    index: IndexSharedInfo;
    toggleLocationDetails: () => void;
    errorsUrl: string;
}

function InlineDetails(props: InlineDetailsProps) {
    const { index, toggleLocationDetails, errorsUrl } = props;

    const estimatedEntries = IndexUtils.estimateEntriesCount(index)?.toLocaleString() ?? "-";
    const errorsCount = index.nodesInfo.filter((x) => x.details).reduce((prev, x) => prev + x.details.errorCount, 0);

    return (
        <>
            <RichPanelDetailItem>
                <Icon icon="list" />
                Entries
                <div className="value">{estimatedEntries}</div>
            </RichPanelDetailItem>
            {errorsCount > 0 && (
                <a href={errorsUrl} title="View indexing errors">
                    <RichPanelDetailItem className="index-detail-item text-danger">
                        <Icon icon="warning" />
                        Errors
                        <div className="value">{errorsCount.toLocaleString()}</div>
                    </RichPanelDetailItem>
                </a>
            )}
            <RichPanelDetailItem>
                <JoinedIndexProgress index={index} onClick={toggleLocationDetails} />
            </RichPanelDetailItem>
        </>
    );
}

interface ReferencedCollectionsProps {
    collections: string[];
}

function ReferencedCollections({ collections }: ReferencedCollectionsProps) {
    if (!collections?.length) {
        return null;
    }

    return (
        <RichPanelDetailItem>
            <Icon icon="referenced-collections" title="Referenced collections" />
            <div style={{ maxWidth: "300px" }}>{collections.join(", ")}</div>
        </RichPanelDetailItem>
    );
}

export default ReferencedCollections;

const indexUniqueId = (index: IndexSharedInfo) => "index_" + index.name;
