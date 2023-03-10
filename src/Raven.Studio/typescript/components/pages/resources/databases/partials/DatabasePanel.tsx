import React, { useState } from "react";
import { DatabaseLocalInfo, DatabaseSharedInfo, ShardedDatabaseSharedInfo } from "components/models/databases";
import classNames from "classnames";
import { useAppUrls } from "hooks/useAppUrls";
import DatabaseLockMode = Raven.Client.ServerWide.DatabaseLockMode;
import {
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
import {
    RichPanel,
    RichPanelActions,
    RichPanelHeader,
    RichPanelInfo,
    RichPanelName,
    RichPanelSelect,
    RichPanelStatus,
} from "components/common/RichPanel";
import { NodeSet, NodeSetItem, NodeSetLabel } from "components/common/NodeSet";
import { useAppDispatch, useAppSelector } from "components/store";
import {
    changeDatabasesLockMode,
    compactDatabase,
    confirmDeleteDatabases,
    confirmSetLockMode,
    confirmToggleDatabases,
    confirmToggleIndexing,
    confirmTogglePauseIndexing,
    deleteDatabases,
    selectActiveDatabase,
    selectDatabaseState,
    toggleDatabases,
    toggleIndexing,
    togglePauseIndexing,
} from "components/common/shell/databasesSlice";
import { useEventsCollector } from "hooks/useEventsCollector";
import useBoolean from "hooks/useBoolean";
import { DatabaseDistribution } from "components/pages/resources/databases/partials/DatabaseDistribution";
import { ValidDatabasePropertiesPanel } from "components/pages/resources/databases/partials/ValidDatabasePropertiesPanel";
import { DatabaseNodeSetItem } from "components/pages/resources/databases/partials/DatabaseNodeSetItem";
import { locationAwareLoadableData } from "components/models/common";
import { useAccessManager } from "hooks/useAccessManager";
import DatabaseUtils from "components/utils/DatabaseUtils";
import assertUnreachable from "components/utils/assertUnreachable";
import { selectEffectiveDatabaseAccessLevel } from "components/common/shell/accessManagerSlice";

interface DatabasePanelProps {
    db: DatabaseSharedInfo;
    selected: boolean;
    toggleSelection: () => void;
}

function getStatusColor(db: DatabaseSharedInfo, localInfo: locationAwareLoadableData<DatabaseLocalInfo>[]) {
    if (localInfo.some((x) => x.status === "success" && x.data.loadError)) {
        return "danger";
    }
    if (localInfo.every((x) => x.status === "success" && !x.data.upTime)) {
        return "secondary";
    }
    if (db.disabled) {
        return "warning";
    }
    return "success";
}

function badgeText(db: DatabaseSharedInfo, localInfo: locationAwareLoadableData<DatabaseLocalInfo>[]) {
    if (localInfo.some((x) => x.status === "success" && x.data.loadError)) {
        return "Error";
    }
    if (localInfo.every((x) => x.status === "success" && !x.data.upTime)) {
        return "Offline";
    }

    if (db.disabled) {
        return "Disabled";
    }

    return "Online";
}

interface DatabaseTopologyProps {
    db: DatabaseSharedInfo;
}

export function AccessIcon(props: { dbAccess: databaseAccessLevel }) {
    const { dbAccess } = props;
    switch (dbAccess) {
        case "DatabaseAdmin":
            return (
                <span title="Admin Access">
                    <i className="icon-access-admin" />
                </span>
            );
        case "DatabaseReadWrite":
            return (
                <span title="Read/Write Access">
                    <i className="icon-access-read-write" />
                </span>
            );
        case "DatabaseRead":
            return (
                <span title="Read-only Access">
                    <i className="icon-access-read" />
                </span>
            );
        default:
            assertUnreachable(dbAccess);
    }
}

function DatabaseTopology(props: DatabaseTopologyProps) {
    const { db } = props;

    if (db.sharded) {
        const shardedDb = db as ShardedDatabaseSharedInfo;
        return (
            <div>
                <NodeSet color="orchestrator" className="m-1">
                    <NodeSetLabel color="orchestrator" icon="orchestrator">
                        Orchestrators
                    </NodeSetLabel>
                    {db.nodes.map((node) => (
                        <DatabaseNodeSetItem key={node.tag} node={node} />
                    ))}
                </NodeSet>

                {shardedDb.shards.map((shard) => {
                    return (
                        <React.Fragment key={shard.name}>
                            <NodeSet color="shard" className="m-1">
                                <NodeSetLabel color="shard" icon="shard">
                                    #{DatabaseUtils.shardNumber(shard.name)}
                                </NodeSetLabel>
                                {shard.nodes.map((node) => (
                                    <DatabaseNodeSetItem key={node.tag} node={node} />
                                ))}
                                {shard.deletionInProgress.map((node) => {
                                    return (
                                        <NodeSetItem
                                            key={"deletion-" + node}
                                            icon="trash"
                                            color="warning"
                                            title="Deletion in progress"
                                            extraIconClassName="pulse"
                                        >
                                            {node}
                                        </NodeSetItem>
                                    );
                                })}
                            </NodeSet>
                        </React.Fragment>
                    );
                })}
            </div>
        );
    } else {
        return (
            <div>
                <NodeSet className="m-1">
                    <NodeSetLabel icon="database">Nodes</NodeSetLabel>
                    {db.nodes.map((node) => {
                        return <DatabaseNodeSetItem key={node.tag} node={node} />;
                    })}
                    {db.deletionInProgress.map((node) => {
                        return (
                            <NodeSetItem
                                key={"deletion-" + node}
                                icon="trash"
                                color="warning"
                                title="Deletion in progress"
                                extraIconClassName="pulse"
                            >
                                {node}
                            </NodeSetItem>
                        );
                    })}
                </NodeSet>
            </div>
        );
    }
}

export function DatabasePanel(props: DatabasePanelProps) {
    const { db, selected, toggleSelection } = props;
    const activeDatabase = useAppSelector(selectActiveDatabase);
    const dbState = useAppSelector(selectDatabaseState(db.name));
    const { appUrl } = useAppUrls();
    const dispatch = useAppDispatch();

    //TODO: review data-bind!
    //TODO: review action access for non-admin users!
    //TODO: show commands errors!
    //TODO: cleanance on toggle db state vs disable indexing (and can't reload db)s

    const dbAccess: databaseAccessLevel = useAppSelector(selectEffectiveDatabaseAccessLevel(db.name));

    const { reportEvent } = useEventsCollector();

    const { value: panelCollapsed, toggle: togglePanelCollapsed } = useBoolean(true);

    const [lockChanges, setLockChanges] = useState(false);

    const [inProgressAction, setInProgressAction] = useState<string>(null);

    const localDocumentsUrl = appUrl.forDocuments(null, db.name);
    const documentsUrl = db.currentNode.relevant
        ? localDocumentsUrl
        : appUrl.toExternalDatabaseUrl(db, localDocumentsUrl);

    const localManageGroupUrl = appUrl.forManageDatabaseGroup(db.name);
    const manageGroupUrl = db.currentNode.relevant
        ? localManageGroupUrl
        : appUrl.toExternalDatabaseUrl(db, localManageGroupUrl);

    const { isOperatorOrAbove, isSecuredServer, isAdminAccessOrAbove } = useAccessManager();

    const canNavigateToDatabase = !db.disabled;

    const indexingDisabled = dbState.some((x) => x.status === "success" && x.data.indexingStatus === "Disabled");
    const canPauseAnyIndexing = dbState.some((x) => x.status === "success" && x.data.indexingStatus === "Running");
    const canResumeAnyPausedIndexing = dbState.some(
        (x) => x.status === "success" && x.data?.indexingStatus === "Paused"
    );

    const canDisableIndexing = isOperatorOrAbove() && !indexingDisabled;
    const canEnableIndexing = isOperatorOrAbove() && indexingDisabled;

    const onChangeLockMode = async (lockMode: DatabaseLockMode) => {
        if (db.lockMode === lockMode) {
            return;
        }

        const dbs = [db];

        reportEvent("databases", "set-lock-mode", lockMode);

        const can = await dispatch(confirmSetLockMode());

        if (can) {
            setLockChanges(true);
            try {
                await dispatch(changeDatabasesLockMode(dbs, lockMode));
            } finally {
                setLockChanges(false);
            }
        }
    };

    const onTogglePauseIndexing = async (pause: boolean) => {
        reportEvent("databases", "pause-indexing");

        const confirmation = await dispatch(confirmTogglePauseIndexing(db, pause));

        if (confirmation.can) {
            try {
                setInProgressAction(pause ? "Pausing indexing" : "Resume indexing");
                await dispatch(togglePauseIndexing(db, pause, confirmation.locations));
            } finally {
                setInProgressAction(null);
            }
        }
    };

    const onToggleDisableIndexing = async (disable: boolean) => {
        reportEvent("databases", "toggle-indexing");

        const confirmation = await dispatch(confirmToggleIndexing(db, disable));

        if (confirmation.can) {
            try {
                setInProgressAction(disable ? "Disabling indexing" : "Enabling indexing");
                await dispatch(toggleIndexing(db, disable));
            } finally {
                setInProgressAction(null);
            }
        }
    };

    const onDelete = async () => {
        const confirmation = await dispatch(confirmDeleteDatabases([db]));

        if (confirmation.can) {
            await dispatch(deleteDatabases(confirmation.databases, confirmation.keepFiles));
        }
    };
    const onCompactDatabase = async () => {
        reportEvent("databases", "compact");
        dispatch(compactDatabase(db));
    };

    const onToggleDatabase = async () => {
        const enable = db.disabled;

        const confirmation = await dispatch(confirmToggleDatabases([db], enable));
        if (confirmation) {
            await dispatch(toggleDatabases([db], enable));
        }
    };

    return (
        <RichPanel
            className={classNames("flex-row", {
                active: activeDatabase === db.name,
                relevant: true,
            })}
        >
            <RichPanelStatus color={getStatusColor(db, dbState)}>{badgeText(db, dbState)}</RichPanelStatus>
            <div className="flex-grow-1">
                <div className="flex-grow-1">
                    <RichPanelHeader>
                        <RichPanelInfo>
                            <RichPanelSelect>
                                <Input type="checkbox" checked={selected} onChange={toggleSelection} />
                            </RichPanelSelect>

                            <RichPanelName>
                                {canNavigateToDatabase ? (
                                    <a
                                        href={documentsUrl}
                                        className={classNames(
                                            { "link-disabled": db.currentNode.isBeingDeleted },
                                            { "link-shard": db.sharded }
                                        )}
                                        target={db.currentNode.relevant ? undefined : "_blank"}
                                        title={db.name}
                                    >
                                        <i
                                            className={classNames(
                                                { "icon-database": !db.sharded },
                                                { "icon-sharding": db.sharded },
                                                { "icon-addon-home": db.currentNode.relevant }
                                            )}
                                        ></i>
                                        <span>{db.name}</span>
                                    </a>
                                ) : (
                                    <span title="Database is disabled">
                                        <i
                                            className={
                                                db.currentNode.relevant
                                                    ? "icon-database icon-addon-home"
                                                    : "icon-database"
                                            }
                                        ></i>
                                        <span>{db.name}</span>
                                    </span>
                                )}
                            </RichPanelName>
                            <div className="text-muted">
                                {dbAccess && isSecuredServer() && <AccessIcon dbAccess={dbAccess} />}
                            </div>
                        </RichPanelInfo>

                        <RichPanelActions>
                            <Button
                                href={manageGroupUrl}
                                title="Manage the Database Group"
                                target={db.currentNode.relevant ? undefined : "_blank"}
                                className="me-1"
                                disabled={!canNavigateToDatabase || db.currentNode.isBeingDeleted}
                            >
                                <i className="icon-dbgroup icon-addon-settings me-2" />
                                Manage group
                            </Button>

                            <UncontrolledDropdown className="me-1">
                                <ButtonGroup>
                                    <Button onClick={onToggleDatabase}>
                                        {db.disabled ? (
                                            <span>
                                                <i className="icon-database-cutout icon-addon-play2 me-1" /> Enable
                                            </span>
                                        ) : (
                                            <span>
                                                <i className="icon-database-cutout icon-addon-cancel me-1" /> Disable
                                            </span>
                                        )}
                                    </Button>
                                    <DropdownToggle caret></DropdownToggle>
                                </ButtonGroup>
                                <DropdownMenu end>
                                    {canPauseAnyIndexing && (
                                        <DropdownItem onClick={() => onTogglePauseIndexing(true)}>
                                            <i className="icon-pause me-1" /> Pause indexing
                                        </DropdownItem>
                                    )}
                                    {canResumeAnyPausedIndexing && (
                                        <DropdownItem onClick={() => onTogglePauseIndexing(false)}>
                                            <i className="icon-play me-1" /> Resume indexing
                                        </DropdownItem>
                                    )}
                                    {canDisableIndexing && (
                                        <DropdownItem onClick={() => onToggleDisableIndexing(true)}>
                                            <i className="icon-stop me-1" /> Disable indexing
                                        </DropdownItem>
                                    )}
                                    {canEnableIndexing && (
                                        <DropdownItem onClick={() => onToggleDisableIndexing(false)}>
                                            <i className="icon-play me-1" /> Enable indexing
                                        </DropdownItem>
                                    )}
                                    <DropdownItem divider />
                                    <DropdownItem onClick={onCompactDatabase}>
                                        <i className="icon-compact me-1" /> Compact database
                                    </DropdownItem>
                                </DropdownMenu>
                            </UncontrolledDropdown>

                            {/* TODO
                            <Button className="me-1">
                                <i className="icon-refresh-stats" />
                            </Button> 
                             <button className="btn btn-success"
                                    data-bind="click: _.partial($root.updateDatabaseInfo, name), enable: canNavigateToDatabase(), disable: isBeingDeleted"
                                    title="Refresh database statistics">
                                <i className="icon-refresh-stats"/>
                            </button>*/}

                            <UncontrolledDropdown>
                                {isOperatorOrAbove() && (
                                    <ButtonGroup>
                                        <Button
                                            onClick={() => onDelete()}
                                            title={
                                                db.lockMode === "Unlock"
                                                    ? "Remove database"
                                                    : "Database cannot be deleted because of the set lock mode"
                                            }
                                            color={db.lockMode === "Unlock" && "danger"}
                                            disabled={db.lockMode !== "Unlock"}
                                            data-bind=" disable: isBeingDeleted() || lockMode() !== 'Unlock', 
                                        css: { 'btn-spinner': isBeingDeleted() || _.includes($root.spinners.localLockChanges(), name) }"
                                        >
                                            {lockChanges && <Spinner size="sm" />}
                                            {!lockChanges && db.lockMode === "Unlock" && <i className="icon-trash" />}
                                            {!lockChanges && db.lockMode === "PreventDeletesIgnore" && (
                                                <i className="icon-trash-cutout icon-addon-cancel" />
                                            )}
                                            {!lockChanges && db.lockMode === "PreventDeletesError" && (
                                                <i className="icon-trash-cutout icon-addon-exclamation" />
                                            )}
                                        </Button>
                                        <DropdownToggle
                                            caret
                                            color={db.lockMode === "Unlock" && "danger"}
                                        ></DropdownToggle>
                                    </ButtonGroup>
                                )}

                                <DropdownMenu>
                                    <DropdownItem
                                        onClick={() => onChangeLockMode("Unlock")}
                                        title="Allow to delete database"
                                    >
                                        <i className="icon-trash-cutout icon-addon-check" /> Allow database delete
                                    </DropdownItem>
                                    <DropdownItem
                                        onClick={() => onChangeLockMode("PreventDeletesIgnore")}
                                        title="Prevent deletion of database. An error will not be thrown if an app attempts to delete the database."
                                    >
                                        <i className="icon-trash-cutout icon-addon-cancel" /> Prevent database delete
                                    </DropdownItem>
                                    <DropdownItem
                                        onClick={() => onChangeLockMode("PreventDeletesError")}
                                        title="Prevent deletion of database. An error will be thrown if an app attempts to delete the database."
                                    >
                                        <i className="icon-trash-cutout icon-addon-exclamation" /> Prevent database
                                        delete (Error)
                                    </DropdownItem>
                                </DropdownMenu>
                            </UncontrolledDropdown>
                            <Button
                                color="secondary"
                                onClick={togglePanelCollapsed}
                                title="Toggle distribution details"
                                className="ms-1"
                            >
                                <i className={panelCollapsed ? "icon-arrow-down" : "icon-arrow-up"} />
                            </Button>
                        </RichPanelActions>
                    </RichPanelHeader>

                    <ValidDatabasePropertiesPanel db={db} />
                    <div className="px-3 pb-2">
                        <Collapse isOpen={!panelCollapsed}>
                            <DatabaseDistribution db={db} />
                        </Collapse>
                        <Collapse isOpen={panelCollapsed}>
                            <DatabaseTopology db={db} />
                        </Collapse>
                    </div>
                </div>
            </div>
        </RichPanel>
    );
}
