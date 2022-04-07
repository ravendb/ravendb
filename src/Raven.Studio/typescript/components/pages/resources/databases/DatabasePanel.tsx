import React, { MouseEventHandler, useCallback, useState } from "react";
import { DatabaseSharedInfo } from "../../../models/databases";
import classNames from "classnames";
import { useActiveDatabase } from "../../../hooks/useActiveDatabase";
import { useAppUrls } from "../../../hooks/useAppUrls";
import databaseInfo from "models/resources/info/databaseInfo";
import deleteDatabaseConfirm from "viewmodels/resources/deleteDatabaseConfirm";
import deleteDatabaseCommand from "commands/resources/deleteDatabaseCommand";
import app from "durandal/app";
import { useEventsCollector } from "../../../hooks/useEventsCollector";
import DatabaseLockMode = Raven.Client.ServerWide.DatabaseLockMode;
import { useServices } from "../../../hooks/useServices";

interface DatabasePanelProps {
    db: DatabaseSharedInfo;
    selected: boolean;
    toggleSelection: () => void;
}


function badgeClass(db: DatabaseSharedInfo) {
    /* TODO
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

function deleteDatabases(toDelete: databaseInfo[]) {
    const confirmDeleteViewModel = new deleteDatabaseConfirm(toDelete);
    confirmDeleteViewModel
        .result
        .done((confirmResult: deleteDatabaseConfirmResult) => {
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

                new deleteDatabaseCommand(toDelete.map(x => x.name), !confirmResult.keepFiles)
                    .execute();
            }
        });

    app.showBootstrapDialog(confirmDeleteViewModel);
}

export function DatabasePanel(props: DatabasePanelProps) {
    const { db, selected, toggleSelection } = props;
    const { db: activeDatabase } = useActiveDatabase();
    const { appUrl } = useAppUrls();
    const eventsCollector = useEventsCollector();
    const { databasesService } = useServices();
    
    const [lockChanges, setLockChanges] = useState(false);
    
    const documentsUrl = appUrl.forDocuments(null, db.name);
    const manageGroupUrl = appUrl.forManageDatabaseGroup(db.name);
    
    //TODO: 
    const deleteDatabase = useCallback(() => deleteDatabases([{
        name: db.name,
        isEncrypted: () => db.encrypted
    } as any]), [db]);
    
    const updateDatabaseLockMode = useCallback(async (db: DatabaseSharedInfo, lockMode: DatabaseLockMode) => {
        if (db.lockMode === lockMode) {
            return;
        }
        
        setLockChanges(true);
        try {
            await databasesService.setLockMode(db, lockMode);
        } finally {
            setLockChanges(false);
        }
    }, [db]);
    
    const allowDatabaseDelete: MouseEventHandler<HTMLElement> = useCallback(async (e) => {
        e.preventDefault();

        eventsCollector.reportEvent("databases", "set-lock-mode", "Unlock");
        await updateDatabaseLockMode(db, "Unlock");
    }, [db]);
    
    const preventDatabaseDelete: MouseEventHandler<HTMLElement> = useCallback(async (e) => {
        e.preventDefault();

        eventsCollector.reportEvent("databases", "set-lock-mode", "LockedIgnore");
        await updateDatabaseLockMode(db, "PreventDeletesIgnore");
    }, [db]);
    
    const preventDatabaseDeleteWithError: MouseEventHandler<HTMLElement> = useCallback(async (e) => {
        e.preventDefault();

        eventsCollector.reportEvent("databases", "set-lock-mode", "LockedError");
        await updateDatabaseLockMode(db, "PreventDeletesError");
    }, [db]);
    
    
    //TODO: createIsLocalDatabaseObservable -> relevant
    return (
        <div className={classNames("panel panel-hover panel-state database-item", badgeClass(db), { active: activeDatabase?.name === db.name, relevant: true })} 
             data-bind="click: $root.databasePanelClicked, scrollTo: isCurrentlyActiveDatabase(), 
                           ) }">
            <div className={classNames("state", badgeClass(db))} data-state-text={badgeText(db)}
    data-bind="attr: { 'data-state-text': $root.createIsLocalDatabaseObservable(name)() ? badgeText : 'remote', 
    class: 'state ' + ($root.createIsLocalDatabaseObservable(name)() ? badgeClass() : 'state-remote') }"/>
            <div className="padding">
                <div className="database-header">
                    <div className="info-container flex-horizontal">
                        <div className="checkbox">
                            <input type="checkbox" className="styled" checked={selected} onChange={toggleSelection} />
                            <label/>
                        </div>
                        <div className="name">
                            {/* TODO <span title="Database is disabled" data-bind="visible: !canNavigateToDatabase()">
                                <small><i
                                    data-bind="attr: { class: $root.createIsLocalDatabaseObservable(name)() ? 'icon-database-home': 'icon-database' }" /></small>
                                <span>{db.name}</span>
                            </span>*/}
                            <a data-bind="attr: {  target: $root.createIsLocalDatabaseObservable(name)() ? undefined : '_blank' },
                                              css: { 'link-disabled': isBeingDeleted }, visible: canNavigateToDatabase()"
                               href={documentsUrl} title={db.name}>
                                <small><i className="icon-database-home"
                                    data-bind="attr: { class: $root.createIsLocalDatabaseObservable(name)() ? 'icon-database-home': 'icon-database' }" /></small>
                                <span>{db.name}</span>
                            </a>
                            { db.sharded && (
                                <span className="text-muted margin-left margin-left-sm">(sharded)</span>
                            )}
                        </div>
                        <div className="member">
                            { /* ko foreach: _.slice(nodes(), 0, 5) */ }
                            <a data-bind="attr: { href: $root.createAllDocumentsUrlObservableForNode($parent, $data), target: tag() === $root.clusterManager.localNodeTag() ? undefined : '_blank',
                                                      title: 'Click to navigate to this database on node ' + tag() },
                                              css: { 'link-disabled': $parent.isBeingDeleted }">
                                <small><i data-bind="attr: { class: cssIcon }" /><span
                                    data-bind="text: 'Node ' + tag()" /></small>
                            </a>
                            { /* /ko --> */ }
                            
                            { /* TODO: <!-- ko foreach: deletionInProgress -->
                            <div>
                                <div title="Deletion in progress" className="text-warning pulse">
                                    <small><i className="icon-trash" /><span data-bind="text: 'Node ' + $data" /></small>
                                </div>
                            </div>
                            <!-- /ko -->*/ }

                            { /* TODO
                            <div data-bind="visible: nodes().length > 5">
                                <a href="#" data-bind="attr: { href: $root.createManageDbGroupUrlObsevable($data) }"
                                   data-toggle="more-nodes-tooltip">
                                    <small><i className="icon-dbgroup"/><span>+<span
    data-bind="text: nodes().length - 5"/> more</span></small>
                                </a>
                            </div> */ }
                        </div>
                    </div>
                    <div className="actions-container flex-grow">
                        { /* TODO
                        <span data-bind="visible: isLoading">
                            <span className="global-spinner spinner-sm"/>&nbsp;&nbsp;&nbsp;&nbsp;
                        </span>
                        */}
                        <div className="actions">
                            <a className="btn btn-default" href={manageGroupUrl} title="Manage the Database Group"
                               data-bind="css: { 'disabled': !canNavigateToDatabase() || isBeingDeleted() }, 
                                              attr: { href: $root.createManageDbGroupUrlObsevable($data), target: $root.createIsLocalDatabaseObservable(name)() ? undefined : '_blank' }">
                                <i className="icon-manage-dbgroup"/>
                                <span>Manage group</span>
                            </a>
                            
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
                            <div className="btn-group" data-bind="visible: $root.accessManager.canDelete">
                                <button type="button" onClick={deleteDatabase} 
                                        title={db.lockMode === "Unlock" ? 'Remove database' : 'Database cannot be deleted because of the set lock mode' }
                                        className={classNames("btn", { "btn-danger": db.lockMode === "Unlock", "btn-default": db.lockMode !== "Unlock", "btn-spinner": lockChanges })}
                                        disabled={db.lockMode !== "Unlock"}
                                        data-bind=" disable: isBeingDeleted() || lockMode() !== 'Unlock',
                                                                   css: { 'btn-spinner': isBeingDeleted() || _.includes($root.spinners.localLockChanges(), name) }">
                                    { db.lockMode === "Unlock" && (
                                        <i className="icon-trash" />
                                    )}
                                    { db.lockMode === "PreventDeletesIgnore" && (
                                        <i className="icon-trash-cutout icon-addon-cancel" />
                                    )}
                                    { db.lockMode === "PreventDeletesError" && (
                                        <i className="icon-trash-cutout icon-addon-exclamation" />
                                    )}
                                </button>
                                <button type="button" 
                                        className={classNames("btn dropdown-toggle", { "btn-danger": db.lockMode === "Unlock", "btn-default": db.lockMode !== "Unlock" })} 
                                        data-toggle="dropdown"
                                        aria-haspopup="true">
                                    <span className="caret"/>
                                    <span className="sr-only">Toggle Dropdown</span>
                                </button>
                                <ul className="dropdown-menu dropdown-menu-right">
                                    <li>
                                        <a href="#" onClick={allowDatabaseDelete}
                                           title="Allow to delete database">
                                            <i className="icon-trash-cutout icon-addon-check"/> Allow database delete
                                        </a>
                                    </li>
                                    <li>
                                        <a href="#" onClick={preventDatabaseDelete}
                                           title="Prevent deletion of database. An error will not be thrown if an app attempts to delete the database.">
                                            <i className="icon-trash-cutout icon-addon-cancel" /> Prevent database
                                            delete
                                        </a>
                                    </li>
                                    <li>
                                        <a href="#" onClick={preventDatabaseDeleteWithError}
                                           title="Prevent deletion of database. An error will be thrown if an app attempts to delete the database.">
                                            <i className="icon-trash-cutout icon-addon-exclamation"/> Prevent
                                            database delete (Error)
                                        </a>
                                    </li>
                                </ul>
                            </div>
                        </div>
                    </div>
                </div>
            </div>
            <ValidDatabasePropertiesPanel db={db} />
            
        </div>
    )
}

interface ValidDatabasePropertiesPanelProps {
    db: DatabaseSharedInfo;
}

function ValidDatabasePropertiesPanel(props: ValidDatabasePropertiesPanelProps) {
    const { db } = props;
    
    return (
        <div className="panel-addon"
             data-bind="template: { name: hasLoadError() ? 'invalid-database-properties-template': 'valid-database-properties-template' }, visible: $root.createIsLocalDatabaseObservable(name)">
            <div className="padding">
                <div className="addons-container flex-wrap">
                    <div className="database-properties">
                        <div className="encryption">
                            { db.encrypted && (
                                <small title="This database is encrypted">
                                    <i className="icon-key text-success"/>
                                </small>
                            )}
                            { !db.encrypted && (
                                <small title="This database is not encrypted">
                                    <i className="icon-unencrypted text-muted"/>
                                </small>
                            )}
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
                    </div>
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
                </div>
            </div>
        </div>
    )
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
