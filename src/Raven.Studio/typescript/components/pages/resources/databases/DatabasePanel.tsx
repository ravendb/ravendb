import React, { MouseEventHandler, useCallback, useState } from "react";
import { DatabaseSharedInfo, ShardedDatabaseSharedInfo } from "../../../models/databases";
import classNames from "classnames";
import { useActiveDatabase } from "hooks/useActiveDatabase";
import { useAppUrls } from "hooks/useAppUrls";
import deleteDatabaseConfirm from "viewmodels/resources/deleteDatabaseConfirm";
import deleteDatabaseCommand from "commands/resources/deleteDatabaseCommand";
import app from "durandal/app";
import { useEventsCollector } from "hooks/useEventsCollector";
import DatabaseLockMode = Raven.Client.ServerWide.DatabaseLockMode;
import { useServices } from "hooks/useServices";
import {
    Badge,
    Button,
    ButtonGroup,
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
    RichPanelDetailItem,
    RichPanelDetails,
    RichPanelHeader,
    RichPanelInfo,
    RichPanelName,
    RichPanelSelect,
    RichPanelStatus,
} from "../../../common/RichPanel";
import appUrl from "common/appUrl";
import { NodeSet, NodeSetItem, NodeSetLabel } from "components/common/NodeSet";
import assertUnreachable from "components/utils/assertUnreachable";

interface DatabasePanelProps {
    db: DatabaseSharedInfo;
    selected: boolean;
    toggleSelection: () => void;
}

function getStatusColor(db: DatabaseSharedInfo) {
    return "success";
}

//TODO:
// eslint-disable-next-line @typescript-eslint/no-unused-vars
function badgeClass(db: DatabaseSharedInfo) {
    /* TODO Created getStatusColor() function this one might be deprecated
     if (this.hasLoadError()) {
                return "state-danger";
            }

            if (this.disabled()) {
                return "state-warning";
            }

            if (this.online()) {
                return "state-success";
            }

            return "state-offline"; // offline
     */
    return "state-success";
}

// eslint-disable-next-line @typescript-eslint/no-unused-vars
function badgeText(db: DatabaseSharedInfo) {
    /* TODO
        if (this.hasLoadError()) {
                return "Error";
            }

            if (this.disabled()) {
                return "Disabled";
            }

            if (this.online()) {
                return "Online";
            }
            return "Offline";
     */

    return "Online";
}

function deleteDatabases(toDelete: DatabaseSharedInfo[]) {
    const confirmDeleteViewModel = new deleteDatabaseConfirm(toDelete);
    confirmDeleteViewModel.result.done((confirmResult: deleteDatabaseConfirmResult) => {
        if (confirmResult.can) {
            /* TODO:
                const dbsList = toDelete.map(x => {
                    //TODO: x.isBeingDeleted(true);
                    const asDatabase = x.asDatabase();

                    // disconnect here to avoid race condition between database deleted message
                    // and websocket disconnection
                    //TODO: changesContext.default.disconnectIfCurrent(asDatabase, "DatabaseDeleted");
                    return asDatabase;
                });*/

            new deleteDatabaseCommand(
                toDelete.map((x) => x.name),
                !confirmResult.keepFiles
            ).execute();
        }
    });

    app.showBootstrapDialog(confirmDeleteViewModel);
}

function toExternalUrl(db: DatabaseSharedInfo, url: string) {
    // we have to redirect to different node, let's find first member where selected database exists
    const firstNode = db.nodes[0];
    if (!firstNode) {
        return "";
    }
    return appUrl.toExternalUrl(firstNode.nodeUrl, url);
}
interface DatabaseTopologyProps {
    db: DatabaseSharedInfo;
}

function extractShardNumber(dbName: string) {
    const [, shard] = dbName.split("$", 2);
    return shard;
}

function DatabaseTopology(props: DatabaseTopologyProps) {
    const { db } = props;

    if (db.sharded) {
        const shardedDb = db as ShardedDatabaseSharedInfo;
        return (
            <div className="px-3 py-2">
                <NodeSet color="warning" className="m-1">
                    <NodeSetLabel color="warning" icon="orchestrator">
                        Orchestrators
                    </NodeSetLabel>
                    {db.nodes.map((node) => (
                        <NodeSetItem key={node.tag} icon={iconForNodeType(node.type)} color="node" title={node.type}>
                            {node.tag}
                        </NodeSetItem>
                    ))}
                </NodeSet>

                {shardedDb.shards.map((shard) => {
                    return (
                        <React.Fragment key={shard.name}>
                            <NodeSet color="shard" className="m-1">
                                <NodeSetLabel color="shard" icon="shard">
                                    #{extractShardNumber(shard.name)}
                                </NodeSetLabel>
                                {shard.nodes.map((node) => {
                                    return (
                                        <NodeSetItem
                                            key={node.tag}
                                            icon={iconForNodeType(node.type)}
                                            color="node"
                                            title={node.type}
                                        >
                                            {node.tag}
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
            <div className="px-3 py-2">
                <NodeSet color="warning" className="m-1">
                    <NodeSetLabel color="primary" icon="database">
                        Nodes
                    </NodeSetLabel>
                    {db.nodes.map((node) => {
                        return (
                            <NodeSetItem
                                key={node.tag}
                                icon={iconForNodeType(node.type)}
                                color="node"
                                title={node.type}
                            >
                                {node.tag}
                            </NodeSetItem>
                        );
                    })}
                </NodeSet>
            </div>
        );
    }
}

function iconForNodeType(type: databaseGroupNodeType) {
    switch (type) {
        case "Member":
            return "dbgroup-member";
        case "Rehab":
            return "dbgroup-rehab";
        case "Promotable":
            return "dbgroup-promotable";
        default:
            assertUnreachable(type);
    }
}

export function DatabasePanel(props: DatabasePanelProps) {
    const { db, selected, toggleSelection } = props;
    const { db: activeDatabase } = useActiveDatabase();
    const { appUrl } = useAppUrls();
    const eventsCollector = useEventsCollector();
    const { databasesService } = useServices();

    const [lockChanges, setLockChanges] = useState(false);

    const localDocumentsUrl = appUrl.forDocuments(null, db.name);
    const documentsUrl = db.currentNode.relevant ? localDocumentsUrl : toExternalUrl(db, localDocumentsUrl);

    const localManageGroupUrl = appUrl.forManageDatabaseGroup(db.name);
    const manageGroupUrl = db.currentNode.relevant ? localManageGroupUrl : toExternalUrl(db, localManageGroupUrl);

    const deleteDatabase = useCallback(() => deleteDatabases([db]), [db]);

    const updateDatabaseLockMode = useCallback(
        async (db: DatabaseSharedInfo, lockMode: DatabaseLockMode) => {
            if (db.lockMode === lockMode) {
                return;
            }

            setLockChanges(true);
            try {
                await databasesService.setLockMode(db, lockMode);
            } finally {
                setLockChanges(false);
            }
        },
        [databasesService]
    );

    const allowDatabaseDelete: MouseEventHandler<HTMLElement> = useCallback(
        async (e) => {
            e.preventDefault();

            eventsCollector.reportEvent("databases", "set-lock-mode", "Unlock");
            await updateDatabaseLockMode(db, "Unlock");
        },
        [db, eventsCollector, updateDatabaseLockMode]
    );

    const preventDatabaseDelete: MouseEventHandler<HTMLElement> = useCallback(
        async (e) => {
            e.preventDefault();

            eventsCollector.reportEvent("databases", "set-lock-mode", "LockedIgnore");
            await updateDatabaseLockMode(db, "PreventDeletesIgnore");
        },
        [db, eventsCollector, updateDatabaseLockMode]
    );

    const preventDatabaseDeleteWithError: MouseEventHandler<HTMLElement> = useCallback(
        async (e) => {
            e.preventDefault();

            eventsCollector.reportEvent("databases", "set-lock-mode", "LockedError");
            await updateDatabaseLockMode(db, "PreventDeletesError");
        },
        [db, eventsCollector, updateDatabaseLockMode]
    );

    const canNavigateToDatabase = !db.currentNode.disabled; //tODO: && !db.currentNode.hasLoadError

    return (
        <RichPanel
            className={classNames("flex-row", badgeClass(db), {
                active: activeDatabase?.name === db.name,
                relevant: true,
            })}
            data-bind="click: $root.databasePanelClicked, scrollTo: isCurrentlyActiveDatabase(), 
                           ) }"
        >
            <RichPanelStatus color={getStatusColor(db)}>{badgeText(db)}</RichPanelStatus>
            <div className="flex-grow-1">
                {/* <div TODO: legacy RichPanelStatus replaced this
                    className={classNames("state", "flex-shrink-0", badgeClass(db))}                    
                    data-bind="attr: { 'data-state-text': $root.createIsLocalDatabaseObservable(name)() ? badgeText : 'remote', 
    class: 'state ' + ($root.createIsLocalDatabaseObservable(name)() ? badgeClass() : 'state-remote') }"
                /> */}
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
                                        className={classNames({ "link-disabled": db.currentNode.isBeingDeleted })}
                                        target={db.currentNode.relevant ? undefined : "_blank"}
                                        title={db.name}
                                    >
                                        <i
                                            className={db.currentNode.relevant ? "icon-database-home" : "icon-database"}
                                        ></i>
                                        <span>{db.name}</span>
                                    </a>
                                ) : (
                                    <div className="name">
                                        <span title="Database is disabled">
                                            <small>
                                                <i
                                                    className={
                                                        db.currentNode.relevant ? "icon-database-home" : "icon-database"
                                                    }
                                                ></i>
                                            </small>
                                            <span>{db.name}</span>
                                        </span>
                                    </div>
                                )}
                            </RichPanelName>

                            {db.sharded && (
                                <Badge color="shard" pill className="me-4">
                                    <i className="icon-sharding" />
                                    <span>sharded</span>
                                </Badge>
                            )}

                            {/* TODO:

                            <Button
                                className="rounded-pill me-1"
                                href="#"
                                target="_blank"
                                title="Click to navigate to this database on node A"
                            >
                                <i className="icon-dbgroup-member me-1" title="Member" />{" "}
                                <strong className="text-node">
                                    <i className="icon-node me-1" />A
                                </strong>
                            </Button>
                            <Button
                                className="rounded-pill me-1"
                                href="#"
                                target="_blank"
                                title="Click to navigate to this database on node B"
                            >
                                <i className="icon-dbgroup-watcher me-1" title="Watcher" />{" "}
                                <strong className="text-node">
                                    <i className="icon-node me-1" />B
                                </strong>
                            </Button>
                            */}

                            <div className="member">
                                {/* ko foreach: _.slice(nodes(), 0, 5) */}
                                {/* <a
                                    data-bind="attr: { href: $root.createAllDocumentsUrlObservableForNode($parent, $data), target: tag() === $root.clusterManager.localNodeTag() ? undefined : '_blank',
                                                      title: 'Click to navigate to this database on node ' + tag() },
                                              css: { 'link-disabled': $parent.isBeingDeleted }"
                                >
                                    <small>
                                        <i data-bind="attr: { class: cssIcon }" />
                                        <span data-bind="text: 'Node ' + tag()" />
                                    </small>
                                </a> */}
                                {/* /ko --> */}

                                {/* TODO: <!-- ko foreach: deletionInProgress -->
                            <div>
                                <div title="Deletion in progress" className="text-warning pulse">
                                    <small><i className="icon-trash" /><span data-bind="text: 'Node ' + $data" /></small>
                                </div>
                            </div>
                            <!-- /ko -->*/}

                                {/* TODO
                            <div data-bind="visible: nodes().length > 5">
                                <a href="#" data-bind="attr: { href: $root.createManageDbGroupUrlObsevable($data) }"
                                   data-toggle="more-nodes-tooltip">
                                    <small><i className="icon-dbgroup"/><span>+<span
    data-bind="text: nodes().length - 5"/> more</span></small>
                                </a>
                            </div> */}
                            </div>
                        </RichPanelInfo>

                        {/* TODO
                        <span data-bind="visible: isLoading">
                            <span className="global-spinner spinner-sm"/>&nbsp;&nbsp;&nbsp;&nbsp;
                        </span>
                        */}
                        <RichPanelActions>
                            <Button
                                href={manageGroupUrl}
                                title="Manage the Database Group"
                                target={db.currentNode.relevant ? undefined : "_blank"}
                                className="me-1"
                                disabled={!canNavigateToDatabase || db.currentNode.isBeingDeleted}
                            >
                                <i className="icon-manage-dbgroup me-2" />
                                Manage group
                            </Button>

                            <UncontrolledDropdown className="me-1" style={{ display: "none" }}>
                                {" "}
                                {/* TODO */}
                                <ButtonGroup>
                                    <Button>
                                        <i className="icon-database-cutout icon-addon-cancel me-1" /> Disable
                                    </Button>
                                    <DropdownToggle caret></DropdownToggle>
                                </ButtonGroup>
                                <DropdownMenu end>
                                    <DropdownItem>
                                        <i className="icon-pause me-1" /> Pause indexing
                                    </DropdownItem>
                                    <DropdownItem>
                                        <i className="icon-stop me-1" /> Disable indexing
                                    </DropdownItem>
                                    <DropdownItem divider />
                                    <DropdownItem>
                                        <i className="icon-compact me-1" /> Compact database
                                    </DropdownItem>
                                </DropdownMenu>
                            </UncontrolledDropdown>

                            {/* TODO
                            <Button className="me-1">
                                <i className="icon-refresh-stats" />
                            </Button> */}

                            {/* TODO <div className="btn-group">
                                <button className="btn btn-default" data-bind="click: $root.toggleDatabase, visible: $root.accessManager.canDisableEnableDatabase,
                                                                    css: { 'btn-spinner': inProgressAction },
                                                                    disable: isBeingDeleted() || inProgressAction()">
                                    <span data-bind="visible: inProgressAction(), text: inProgressAction()"/>
                                    <i className="icon-database-cutout icon-addon-play2"
    data-bind="visible: !inProgressAction() && disabled()"/>
                                    <span data-bind="visible: !inProgressAction() && disabled()">Enable</span>
                                    <i className="icon-database-cutout icon-addon-cancel"
    data-bind="visible: !inProgressAction() && !disabled()"/>
                                    <span data-bind="visible: !inProgressAction() && !disabled()">Disable</span>
                                </button>
                                <button type="button" className="btn btn-default dropdown-toggle" data-toggle="dropdown"
                                        aria-haspopup="true" aria-expanded="false"
                                        data-bind="disable: isBeingDeleted() || inProgressAction(), 
                                                       visible: online() && $root.isAdminAccessByDbName($data.name)">
                                    <span className="caret"/>
                                    <span className="sr-only">Toggle Dropdown</span>
                                </button>
                                <ul className="dropdown-menu dropdown-menu-right">
                                    <li data-bind="visible: online() && !indexingPaused() && !indexingDisabled()">
                                        <a href="#" data-bind="click: $root.togglePauseDatabaseIndexing">
                                            <i className="icon-pause"/> Pause indexing
                                        </a>
                                    </li>
                                    <li data-bind="visible: indexingPaused()">
                                        <a href="#" data-bind="click: $root.togglePauseDatabaseIndexing">
                                            <i className="icon-play"/> Resume indexing
                                        </a>
                                    </li>
                                    <li data-bind="visible: !indexingDisabled() && $root.accessManager.canDisableIndexing()">
                                        <a href="#" data-bind="click: $root.toggleDisableDatabaseIndexing">
                                            <i className="icon-cancel"/> Disable indexing
                                        </a>
                                    </li>
                                    <li data-bind="visible: indexingDisabled() && $root.accessManager.canDisableIndexing()">
                                        <a href="#" data-bind="click: $root.toggleDisableDatabaseIndexing">
                                            <i className="icon-play"/> Enable indexing
                                        </a>
                                    </li>
                                    <li className="divider"
    data-bind="visible: $root.createIsLocalDatabaseObservable(name) &&  $root.accessManager.canCompactDatabase()"/>
                                    <li data-bind="visible: $root.createIsLocalDatabaseObservable(name)() && $root.accessManager.canCompactDatabase()">
                                        <a data-bind="visible: disabled" title="The database is disabled"
                                           className="has-disable-reason disabled" data-placement="top">
                                            <i className="icon-compact"/> Compact database
                                        </a>
                                        <a href="#" data-bind="click: $root.compactDatabase, visible: !disabled()">
                                            <i className="icon-compact"/> Compact database
                                        </a>
                                    </li>
                                </ul>
                            </div>*/}
                            {/* TODO <button className="btn btn-success"
                                    data-bind="click: _.partial($root.updateDatabaseInfo, name), enable: canNavigateToDatabase(), disable: isBeingDeleted"
                                    title="Refresh database statistics">
                                <i className="icon-refresh-stats"/>
                            </button>*/}

                            <UncontrolledDropdown>
                                <ButtonGroup data-bind="visible: $root.accessManager.canDelete">
                                    <Button
                                        onClick={deleteDatabase}
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
                                    <DropdownToggle caret color={db.lockMode === "Unlock" && "danger"}></DropdownToggle>
                                </ButtonGroup>
                                <DropdownMenu>
                                    <DropdownItem onClick={allowDatabaseDelete} title="Allow to delete database">
                                        <i className="icon-trash-cutout icon-addon-check" /> Allow database delete
                                    </DropdownItem>
                                    <DropdownItem
                                        onClick={preventDatabaseDelete}
                                        title="Prevent deletion of database. An error will not be thrown if an app attempts to delete the database."
                                    >
                                        <i className="icon-trash-cutout icon-addon-cancel" /> Prevent database delete
                                    </DropdownItem>
                                    <DropdownItem
                                        onClick={preventDatabaseDeleteWithError}
                                        title="Prevent deletion of database. An error will be thrown if an app attempts to delete the database."
                                    >
                                        <i className="icon-trash-cutout icon-addon-exclamation" /> Prevent database
                                        delete (Error)
                                    </DropdownItem>
                                </DropdownMenu>
                            </UncontrolledDropdown>
                        </RichPanelActions>
                    </RichPanelHeader>

                    <ValidDatabasePropertiesPanel db={db} />

                    <DatabaseTopology db={db} />
                </div>
            </div>
        </RichPanel>
    );
}

interface ValidDatabasePropertiesPanelProps {
    db: DatabaseSharedInfo;
}

function ValidDatabasePropertiesPanel(props: ValidDatabasePropertiesPanelProps) {
    const { db } = props;

    return (
        <RichPanelDetails
            className="flex-wrap"
            data-bind="template: { name: hasLoadError() ? 'invalid-database-properties-template': 'valid-database-properties-template' }, visible: $root.createIsLocalDatabaseObservable(name)"
        >
            <RichPanelDetailItem>
                <div className="encryption">
                    {db.encrypted && (
                        <small title="This database is encrypted">
                            <i className="icon-key text-success" />
                        </small>
                    )}
                    {!db.encrypted && (
                        <small title="This database is not encrypted">
                            <i className="icon-unencrypted text-muted" />
                        </small>
                    )}
                </div>
            </RichPanelDetailItem>
            {/*  TODO
            <RichPanelDetailItem>
                <i className="icon-drive me-1" /> 138.37 MB 
            </RichPanelDetailItem>
            <RichPanelDetailItem>
                <i className="icon-documents me-1" /> 1,060 
            </RichPanelDetailItem>
            <RichPanelDetailItem>
                <i className="icon-index me-1" /> 0
            </RichPanelDetailItem>
            <RichPanelDetailItem>
                <i className="icon-clock me-1" /> Up for 25 minutes 
            </RichPanelDetailItem>
            <RichPanelDetailItem title="Last backup" className="text-danger">
                <i className="icon-backup me-1" /> Never backed up 
            </RichPanelDetailItem>
            */}

            <div className="rich-panel-details-right" style={{ display: "none" }}>
                {" "}
                {/* TODO */}
                <RichPanelDetailItem
                    title="Indexing errors. Click to view the Indexing Errors."
                    className="text-danger"
                >
                    <i className="icon-exclamation me-1" /> Indexing errors {/* TODO */}
                </RichPanelDetailItem>
                <RichPanelDetailItem title="Indexing is paused. Click to view the Index List." className="text-warning">
                    <i className="icon-pause me-1" /> Indexing paused {/* TODO */}
                </RichPanelDetailItem>
                <RichPanelDetailItem title="Indexing is disabled" className="text-danger">
                    <i className="icon-stop me-1" /> Indexing disabled {/* TODO */}
                </RichPanelDetailItem>
                <RichPanelDetailItem title="Click to view alerts in Notification Center" className="text-warning">
                    <i className="icon-warning me-1" /> 3 Alerts {/* TODO */}
                </RichPanelDetailItem>
                <RichPanelDetailItem title="Click to view alerts in Notification Center" className="text-info">
                    <i className="icon-rocket me-1" /> 2 Performance hints {/* TODO */}
                </RichPanelDetailItem>
            </div>
            {/* TODO <div data-bind="if: databaseAccessText">
                            <div className="database-access" title="Database access level">
                                <i data-bind="attr: { class: databaseAccessColor() + ' ' + databaseAccessClass() }"/>
                                <small data-bind="text: databaseAccessText"/>
                            </div>
                        </div>
                        <div className="storage">
                            <small><i className="icon-drive"/></small>
                            <a className="set-size" data-toggle="size-tooltip"
                               data-bind="attr: { href: $root.storageReportUrl($data) }, css: { 'link-disabled': !canNavigateToDatabase() || isBeingDeleted() }">
                                <small
    data-bind="text: $root.formatBytes(totalSize() + totalTempBuffersSize())"/>
                            </a>
                        </div>
                        <div className="documents">
                            <small><i className="icon-document-group"/></small>
                            <a className="set-size" title="Number of documents. Click to view the Document List."
                               data-bind="attr: { href: $root.createAllDocumentsUrlObservable($data)}, css: { 'link-disabled': !canNavigateToDatabase() || isBeingDeleted() },">
                                <small data-bind="text: (documentsCount() || 0).toLocaleString()"/>
                            </a>
                        </div>
                        <div className="indexes">
                            <small><i className="icon-index"/></small>
                            <a className="set-size" title="Number of indexes. Click to view the Index List."
                               data-bind="attr: { href: $root.indexesUrl($data) }, css: { 'link-disabled': !canNavigateToDatabase() || isBeingDeleted() }">
                                <small data-bind="text: (indexesCount() || 0).toLocaleString()"/>
                            </a>
                        </div>
                        <!--ko if: !uptime()-->
                        <div className="uptime text-muted">
                            <small><i className="icon-recent"/></small>
                            <small>Offline</small>
                        </div>
                        <!--/ko-->
                        <!--ko if: uptime()-->
                        <div className="uptime">
                            <small><i className="icon-recent"/></small>
                            <span title="The database uptime">
                        <small className="hidden-compact">Up for</small>
                        <small data-bind="text: uptime()"/>
                    </span>
                        </div>
                        <!--/ko-->
                        <div className="backup">
                            <div className="properties-value value-only">
                                <a className="set-size" title="Click to navigate to Backups view"
                                   data-bind="css: { 'link-disabled': !canNavigateToDatabase() || isBeingDeleted() }, attr: { href: $root.backupsViewUrl($data), class: backupStatus() }">
                                    <small><i className="icon-backup"/></small>
                                    <small data-bind="text: lastBackupText"/>
                                </a>
                            </div>
                        </div>*/}

            {/* TODO <div className="database-properties-right">
                        <div className="indexing-errors text-danger" data-bind="visible: indexingErrors()">
                            <small><i className="icon-exclamation"/></small>
                            <a className="set-size text-danger"
                               title="Indexing errors. Click to view the Indexing Errors."
                               data-bind="attr: { href: $root.indexErrorsUrl($data) }, css: { 'link-disabled': !canNavigateToDatabase() || isBeingDeleted() }">
                                <small data-bind="text: indexingErrors().toLocaleString()"/>
                                <small className="hidden-compact"
    data-bind="text: $root.pluralize(indexingErrors().toLocaleString(), 'indexing error', 'indexing errors', true)"/>
                            </a>
                        </div>
                        <div className="indexing-paused text-warning"
                             data-bind="visible: indexingPaused() && !indexingDisabled()">
                            <small><i className="icon-pause"/></small>
                            <a className="set-size text-warning"
                               title="Indexing is paused. Click to view the Index List."
                               data-bind="attr: { href: $root.indexesUrl($data) }">
                                <small>Indexing paused</small>
                            </a>
                        </div>
                        <div className="indexing-disabled text-danger" data-bind="visible: indexingDisabled()">
                            <span className="set-size" title="Indexing is disabled">
                                <small><i className="icon-lock"/></small>
                                <small>Indexing disabled</small>
                            </span>
                        </div>
                        <div className="alerts text-warning" data-bind="visible: alerts()">
                            <div className="set-size">
                                <small><i className="icon-warning"/></small>
                                <a className="set-size text-warning" title="Click to view alerts in Notification Center"
                                   href="#"
                                   data-bind="click: _.partial($root.openNotificationCenter, $data), css: { 'link-disabled': !canNavigateToDatabase() }">
                                    <small data-bind="text: alerts().toLocaleString()"/>
                                    <small
    data-bind="text: $root.pluralize(alerts().toLocaleString(), 'alert', 'alerts', true)"/>
                                </a>
                            </div>
                        </div>
                        <div className="performance-hints text-info" data-bind="visible: performanceHints()">
                            <div className="set-size">
                                <small><i className="icon-rocket"/></small>
                                <a className="set-size text-info" title="Click to view hints in Notification Center"
                                   href="#"
                                   data-bind="click: _.partial($root.openNotificationCenter, $data), css: { 'link-disabled': !canNavigateToDatabase() }">
                                    <small data-bind="text: performanceHints().toLocaleString()"/>
                                    <small className="hidden-compact"
    data-bind="text: $root.pluralize(performanceHints().toLocaleString(), 'performance hint', 'performance hints', true)"/>
                                </a>
                            </div>
                        </div>
                      
                    </div>*/}
        </RichPanelDetails>
    );
}

/* TODO

<script type="text/html" id="invalid-database-properties-template">
    <div class="padding">
        <div class="addons-container flex-wrap">
            <div class="text-danger flex-grow">
                <small>
                    <i class="icon-exclamation"></i>
                    <span data-bind="text: loadError"></span>
                </small>
            </div>
        </div>
    </div>
</script>
 */
