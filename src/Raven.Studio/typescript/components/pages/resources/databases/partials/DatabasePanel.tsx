import React, { MouseEvent, useState } from "react";
import { DatabaseLocalInfo, DatabaseSharedInfo } from "components/models/databases";
import classNames from "classnames";
import { useAppUrls } from "hooks/useAppUrls";
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
import { useAppDispatch, useAppSelector } from "components/store";
import { useEventsCollector } from "hooks/useEventsCollector";
import useBoolean from "hooks/useBoolean";
import { DatabaseDistribution } from "components/pages/resources/databases/partials/DatabaseDistribution";
import { ValidDatabasePropertiesPanel } from "components/pages/resources/databases/partials/ValidDatabasePropertiesPanel";
import { locationAwareLoadableData } from "components/models/common";
import { useAccessManager } from "hooks/useAccessManager";
import DatabaseUtils from "components/utils/DatabaseUtils";
import { accessManagerSelectors } from "components/common/shell/accessManagerSlice";
import genUtils from "common/generalUtils";
import databasesManager from "common/shell/databasesManager";
import { AccessIcon } from "components/pages/resources/databases/partials/AccessIcon";
import { DatabaseTopology } from "components/pages/resources/databases/partials/DatabaseTopology";
import DatabaseLockMode = Raven.Client.ServerWide.DatabaseLockMode;
import { Icon } from "components/common/Icon";
import { selectDatabaseState } from "components/pages/resources/databases/store/databasesViewSelectors";
import {
    changeDatabasesLockMode,
    compactDatabase,
    confirmSetLockMode,
    confirmToggleDatabases,
    confirmToggleIndexing,
    confirmTogglePauseIndexing,
    reloadDatabaseDetails,
    toggleDatabases,
    toggleIndexing,
    togglePauseIndexing,
} from "components/pages/resources/databases/store/databasesViewActions";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { databaseActions } from "components/common/shell/databaseSliceActions";
import BulkDatabaseResetConfirm from "./BulkDatabaseResetConfirm";
import { clusterSelectors } from "components/common/shell/clusterSlice";
import changesContext = require("common/changesContext");
import { useServices } from "components/hooks/useServices";
import { DatabaseActionContexts } from "components/common/MultipleDatabaseLocationSelector";
import ActionContextUtils from "components/utils/actionContextUtils";

interface DatabasePanelProps {
    databaseName: string;
    selected: boolean;
    toggleSelection: () => void;
}

function getStatusColor(db: DatabaseSharedInfo, localInfo: locationAwareLoadableData<DatabaseLocalInfo>[]) {
    const state = DatabaseUtils.getDatabaseState(db, localInfo);
    switch (state) {
        case "Loading":
            return "secondary";
        case "Error":
            return "danger";
        case "Offline":
            return "secondary";
        case "Disabled":
            return "warning";
        default:
            return "success";
    }
}

function badgeText(db: DatabaseSharedInfo, localInfo: locationAwareLoadableData<DatabaseLocalInfo>[]) {
    const state = DatabaseUtils.getDatabaseState(db, localInfo);
    if (state === "Loading") {
        return "Loading...";
    }

    if (state === "Partially Online") {
        const onlineCount = localInfo.filter((x) => x.status === "success" && x.data.upTime).length;
        return `Online (${onlineCount}/${localInfo.length})`;
    }

    return state;
}

export function DatabasePanel(props: DatabasePanelProps) {
    const { databaseName, selected, toggleSelection } = props;
    const db = useAppSelector(databaseSelectors.databaseByName(databaseName));
    const activeDatabase = useAppSelector(databaseSelectors.activeDatabase);
    const dbState = useAppSelector(selectDatabaseState(db.name));
    const { appUrl } = useAppUrls();
    const dispatch = useAppDispatch();
    const { databasesService } = useServices();

    //TODO: show commands errors!

    const dbAccess: databaseAccessLevel = useAppSelector(accessManagerSelectors.effectiveDatabaseAccessLevel(db.name));
    const localNodeTag = useAppSelector(clusterSelectors.localNodeTag);

    const { reportEvent } = useEventsCollector();

    const { value: panelCollapsed, toggle: togglePanelCollapsed } = useBoolean(true);

    const [lockChanges, setLockChanges] = useState(false);

    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    const [inProgressAction, setInProgressAction] = useState<string>(null);
    const {
        value: isOpenDatabaseRestartConfirm,
        setValue: setIsOpenDatabaseRestartConfirm,
        toggle: toggleIsOpenDatabaseRestartConfirm,
    } = useBoolean(false);

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

    const canRestartDatabase = isAdminAccessOrAbove(db) && !db.disabled;

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
        const confirmation = await dispatch(databaseActions.confirmDeleteDatabases([db]));

        if (confirmation.can) {
            await dispatch(databaseActions.deleteDatabases(confirmation.databases, confirmation.keepFiles));
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

    const onHeaderClicked = async (db: DatabaseSharedInfo, e: MouseEvent<HTMLElement>) => {
        if (genUtils.canConsumeDelegatedEvent(e)) {
            if (!db || db.disabled || !db.currentNode.relevant) {
                return true;
            }

            const manager = databasesManager.default;

            const databaseToActivate = manager.getDatabaseByName(db.name);

            if (databaseToActivate) {
                try {
                    await manager.activate(databaseToActivate);
                    await manager.updateDatabaseInfo(databaseToActivate, db.name);
                } finally {
                    await dispatch(reloadDatabaseDetails(db.name));
                }
            }
        }
    };

    const getRestartDatabaseContextPoints = () => {
        return ActionContextUtils.getContexts(
            DatabaseUtils.getLocations(db),
            db.nodes.map((x) => x.tag)
        );
    };

    const showResetDatabaseConfirmation = async () => {
        reportEvent("databases", "restart-database");
        setIsOpenDatabaseRestartConfirm(true);
    };

    const onRestartDatabase = async (contextPoints: DatabaseActionContexts[]) => {
        changesContext.default.disconnectIfCurrent(db, "DatabaseRestarted");

        for (const { nodeTag, shardNumbers, includeOrchestrator: includeOrchestrator } of contextPoints) {
            if (includeOrchestrator) {
                await databasesService.restartDatabase(db, {
                    nodeTag,
                });
            }

            const locations = ActionContextUtils.getLocations(nodeTag, shardNumbers);
            for (const location of locations) {
                await databasesService.restartDatabase(db, location);
            }
        }
    };

    return (
        <RichPanel
            hover={db.currentNode.relevant}
            className={classNames("flex-row", "with-status", {
                active: activeDatabase === db.name,
                relevant: true,
            })}
        >
            <RichPanelStatus color={getStatusColor(db, dbState)}>{badgeText(db, dbState)}</RichPanelStatus>
            <div className="flex-grow-1">
                <div className="flex-grow-1">
                    <RichPanelHeader onClick={(e) => onHeaderClicked(db, e)}>
                        <RichPanelInfo>
                            {isOperatorOrAbove() && (
                                <RichPanelSelect>
                                    <Input type="checkbox" checked={selected} onChange={toggleSelection} />
                                </RichPanelSelect>
                            )}

                            <RichPanelName className="max-width-heading">
                                {canNavigateToDatabase ? (
                                    <a
                                        href={documentsUrl}
                                        className={classNames(
                                            "d-block text-truncate",
                                            { "link-disabled": db.currentNode.isBeingDeleted },
                                            { "link-shard": db.sharded }
                                        )}
                                        target={db.currentNode.relevant ? undefined : "_blank"}
                                        title={db.name}
                                    >
                                        <Icon
                                            icon={db.sharded ? "sharding" : "database"}
                                            addon={db.currentNode.relevant ? "home" : null}
                                            margin="me-2"
                                        />
                                        {db.name}
                                    </a>
                                ) : (
                                    <span title="Database is disabled" className="d-block text-truncate">
                                        <Icon
                                            icon="database"
                                            addon={db.currentNode.relevant ? "home" : null}
                                            margin="m-0"
                                        />
                                        {db.name}
                                    </span>
                                )}
                            </RichPanelName>
                            <div className="text-muted">
                                {dbAccess && isSecuredServer() && <AccessIcon dbAccess={dbAccess} />}
                            </div>
                        </RichPanelInfo>

                        <RichPanelActions>
                            {isOperatorOrAbove() && (
                                <Button
                                    href={manageGroupUrl}
                                    title="Manage the Database Group"
                                    target={db.currentNode.relevant ? undefined : "_blank"}
                                    disabled={!canNavigateToDatabase || db.currentNode.isBeingDeleted}
                                >
                                    <span>
                                        <Icon icon="dbgroup" addon="settings" /> Manage group
                                    </span>
                                </Button>
                            )}

                            {isAdminAccessOrAbove(db) && (
                                <UncontrolledDropdown>
                                    <ButtonGroup>
                                        {isOperatorOrAbove() && (
                                            <Button onClick={onToggleDatabase}>
                                                {db.disabled ? (
                                                    <span>
                                                        <Icon icon="database" addon="play2" /> Enable
                                                    </span>
                                                ) : (
                                                    <span>
                                                        <Icon icon="database" addon="cancel" /> Disable
                                                    </span>
                                                )}
                                            </Button>
                                        )}
                                        <DropdownToggle caret></DropdownToggle>
                                    </ButtonGroup>

                                    <DropdownMenu end container="dropdownContainer">
                                        {canPauseAnyIndexing && (
                                            <DropdownItem onClick={() => onTogglePauseIndexing(true)}>
                                                <Icon icon="pause" /> Pause indexing until restart
                                            </DropdownItem>
                                        )}
                                        {canResumeAnyPausedIndexing && (
                                            <DropdownItem onClick={() => onTogglePauseIndexing(false)}>
                                                <Icon icon="play" /> Resume indexing
                                            </DropdownItem>
                                        )}
                                        {canDisableIndexing && (
                                            <DropdownItem onClick={() => onToggleDisableIndexing(true)}>
                                                <Icon icon="stop" /> Disable indexing
                                            </DropdownItem>
                                        )}
                                        {canEnableIndexing && (
                                            <DropdownItem onClick={() => onToggleDisableIndexing(false)}>
                                                <Icon icon="play" /> Enable indexing
                                            </DropdownItem>
                                        )}
                                        {isOperatorOrAbove() && (
                                            <>
                                                <DropdownItem divider />
                                                {isAdminAccessOrAbove(db) && (
                                                    <>
                                                        {isOpenDatabaseRestartConfirm && (
                                                            <BulkDatabaseResetConfirm
                                                                dbName={db.name}
                                                                localNodeTag={localNodeTag}
                                                                allActionContexts={getRestartDatabaseContextPoints()}
                                                                toggleConfirmation={toggleIsOpenDatabaseRestartConfirm}
                                                                onConfirm={onRestartDatabase}
                                                            />
                                                        )}
                                                        <DropdownItem
                                                            onClick={showResetDatabaseConfirmation}
                                                            disabled={!canRestartDatabase}
                                                        >
                                                            <Icon icon="reset" /> Restart database
                                                        </DropdownItem>
                                                    </>
                                                )}
                                                <DropdownItem onClick={onCompactDatabase}>
                                                    <Icon icon="compact" /> Compact database
                                                </DropdownItem>
                                            </>
                                        )}
                                    </DropdownMenu>
                                </UncontrolledDropdown>
                            )}

                            {isOperatorOrAbove() && (
                                <UncontrolledDropdown>
                                    <ButtonGroup>
                                        <Button
                                            onClick={() => onDelete()}
                                            title={
                                                db.lockMode === "Unlock"
                                                    ? "Remove database"
                                                    : "Database cannot be deleted because of the set lock mode"
                                            }
                                            color={db.lockMode === "Unlock" ? "danger" : "secondary"}
                                            disabled={db.lockMode !== "Unlock"}
                                        >
                                            {lockChanges && <Spinner size="sm" />}
                                            {!lockChanges && db.lockMode === "Unlock" && (
                                                <Icon icon="trash" margin="m-0" />
                                            )}
                                            {!lockChanges && db.lockMode === "PreventDeletesIgnore" && (
                                                <Icon icon="trash" addon="cancel" margin="m-0" />
                                            )}
                                            {!lockChanges && db.lockMode === "PreventDeletesError" && (
                                                <Icon icon="trash" addon="exclamation" margin="m-0" />
                                            )}
                                        </Button>
                                        <DropdownToggle
                                            caret
                                            color={db.lockMode === "Unlock" ? "danger" : "secondary"}
                                        ></DropdownToggle>
                                    </ButtonGroup>

                                    <DropdownMenu container="dropdownContainer">
                                        <DropdownItem
                                            onClick={() => onChangeLockMode("Unlock")}
                                            title="Allow to delete database"
                                        >
                                            <Icon icon="trash" addon="check" /> Allow database delete
                                        </DropdownItem>
                                        <DropdownItem
                                            onClick={() => onChangeLockMode("PreventDeletesIgnore")}
                                            title="Prevent deletion of database. An error will not be thrown if an app attempts to delete the database."
                                        >
                                            <Icon icon="trash" addon="cancel" /> Prevent database delete
                                        </DropdownItem>
                                        <DropdownItem
                                            onClick={() => onChangeLockMode("PreventDeletesError")}
                                            title="Prevent deletion of database. An error will be thrown if an app attempts to delete the database."
                                        >
                                            <Icon icon="trash" addon="exclamation" /> Prevent database delete (Error)
                                        </DropdownItem>
                                    </DropdownMenu>
                                </UncontrolledDropdown>
                            )}
                        </RichPanelActions>
                    </RichPanelHeader>
                    <ValidDatabasePropertiesPanel
                        db={db}
                        panelCollapsed={panelCollapsed}
                        togglePanelCollapsed={togglePanelCollapsed}
                    />
                    <div className="px-4 pb-2">
                        <Collapse isOpen={!panelCollapsed}>
                            <DatabaseDistribution db={db} />
                        </Collapse>
                        <Collapse isOpen={panelCollapsed}>
                            <DatabaseTopology db={db} togglePanelCollapsed={togglePanelCollapsed} />
                        </Collapse>
                    </div>
                </div>
            </div>
        </RichPanel>
    );
}
