import app = require("durandal/app");
import appUrl = require("common/appUrl");
import viewModelBase = require("viewmodels/viewModelBase");
import accessHelper = require("viewmodels/shell/accessHelper");
import deleteDatabaseConfirm = require("viewmodels/resources/deleteDatabaseConfirm");
import createDatabase = require("viewmodels/resources/createDatabase");
import disableDatabaseToggleConfirm = require("viewmodels/resources/disableDatabaseToggleConfirm");
import toggleDatabaseCommand = require("commands/resources/toggleDatabaseCommand");
import togglePauseIndexingCommand = require("commands/database/index/togglePauseIndexingCommand");
import toggleDisableIndexingCommand = require("commands/database/index/toggleDisableIndexingCommand");
import deleteDatabaseCommand = require("commands/resources/deleteDatabaseCommand");
import loadDatabaseCommand = require("commands/resources/loadDatabaseCommand");
import changesContext = require("common/changesContext");

import databasesInfo = require("models/resources/info/databasesInfo");
import getDatabasesCommand = require("commands/resources/getDatabasesCommand");
import getDatabaseCommand = require("commands/resources/getDatabaseCommand");
import databaseInfo = require("models/resources/info/databaseInfo");
import messagePublisher = require("common/messagePublisher");
import clusterTopologyManager = require("common/shell/clusterTopologyManager");
import databaseGroupNode = require("models/resources/info/databaseGroupNode");

class databases extends viewModelBase {

    databases = ko.observable<databasesInfo>();
    clusterManager = clusterTopologyManager.default;

    filters = {
        searchText: ko.observable<string>(),
        localOnly: ko.observable<string>()
    }

    selectionState: KnockoutComputed<checkbox>;
    selectedDatabases = ko.observableArray<string>([]);

    spinners = {
        globalToggleDisable: ko.observable<boolean>(false)
    }

    private static compactView = ko.observable<boolean>(false);
    compactView = databases.compactView;

    isGlobalAdmin = accessHelper.isGlobalAdmin;
    
    constructor() {
        super();

        this.bindToCurrentInstance("newDatabase", "toggleDatabase", "togglePauseDatabaseIndexing", "toggleDisableDatabaseIndexing", "deleteDatabase", "activateDatabase");

        this.initObservables();
    }

    private initObservables() {
        const filters = this.filters;

        filters.searchText.throttle(200).subscribe(() => this.filterDatabases());
        filters.localOnly.subscribe(() => this.filterDatabases());

        this.selectionState = ko.pureComputed<checkbox>(() => {
            const databases = this.databases().sortedDatabases().filter(x => !x.filteredOut());
            const selectedCount = this.selectedDatabases().length;
            if (databases.length && selectedCount === databases.length)
                return checkbox.Checked;
            if (selectedCount > 0)
                return checkbox.SomeChecked;
            return checkbox.UnChecked;
        });
    }

    // Override canActivate: we can always load this page, regardless of any system db prompt.
    canActivate(args: any): any {
        return true;
    }

    activate(args: any): JQueryPromise<Raven.Client.Server.Operations.DatabasesInfo> {
        super.activate(args);

        // we can't use createNotifications here, as it is called after *database changes API* is connected, but user
        // can enter this view and never select database

        this.addNotification(this.changesContext.serverNotifications().watchAllDatabaseChanges((e: Raven.Server.NotificationCenter.Notifications.Server.DatabaseChanged) => this.onDatabaseChange(e)));
        this.addNotification(this.changesContext.serverNotifications().watchReconnect(() => this.fetchDatabases()));

        return this.fetchDatabases();
    }

    attached() {
        super.attached();
        this.updateHelpLink("Z8DC3Q");
        this.updateUrl(appUrl.forDatabases());
    }

    private fetchDatabases(): JQueryPromise<Raven.Client.Server.Operations.DatabasesInfo> {
        return new getDatabasesCommand()
            .execute()
            .done(info => this.databases(new databasesInfo(info)));
    }

    private onDatabaseChange(e: Raven.Server.NotificationCenter.Notifications.Server.DatabaseChanged) {
        switch (e.ChangeType) {
            case "Load":
            case "Put":
                this.updateDatabaseInfo(e.DatabaseName);
                break;

            case "RemoveNode":
            case "Delete":
                // since we don't know if database was removed from current node, let's fetch databaseInfo first
                this.updateDatabaseInfo(e.DatabaseName)
                    .fail((xhr: JQueryXHR) => {
                        if (xhr.status === 404) {
                            // database was removed from all nodes

                            const db = this.databases().sortedDatabases().find(rs => rs.name === e.DatabaseName);
                            if (db) {
                                this.removeDatabase(db);
                            }
                        }
                    });
                break;
        }
    }

    private updateDatabaseInfo(databaseName: string) {
        return new getDatabaseCommand(databaseName)
            .execute()
            .done((result: Raven.Client.Server.Operations.DatabaseInfo) => {
                this.databases().updateDatabase(result);
                this.filterDatabases();
            });
    }

    private filterDatabases(): void {
        const filters = this.filters;
        let searchText = filters.searchText();
        const hasSearchText = !!searchText;
        const localOnly = filters.localOnly();
        const nodeTag = this.clusterManager.localNodeTag();

        if (hasSearchText) {
            searchText = searchText.toLowerCase();
        }

        const matchesFilters = (rs: databaseInfo) => {
            const matchesText = !hasSearchText || rs.name.toLowerCase().indexOf(searchText) >= 0;
            const matchesLocal = !localOnly || _.some(rs.nodes(), x => x.tag() === nodeTag && (x.type() === "Member" || x.type() === "Promotable"));

            return matchesText && matchesLocal;
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

    createManageDbGroupUrlObsevable(dbInfo: databaseInfo): KnockoutComputed<string> {
        const isLocalObservable = this.createIsLocalDatabaseObservable(dbInfo.name);

        return ko.pureComputed(() => {
            const isLocal = isLocalObservable();
            const link = appUrl.forManageDatabaseGroup(dbInfo);
            if (isLocal) {
                return link;
            } else {
                return databases.toExternalUrl(dbInfo, link);
            }
        });
    }

    createAllDocumentsUrlObservable(dbInfo: databaseInfo): KnockoutComputed<string> {
        const isLocalObservable = this.createIsLocalDatabaseObservable(dbInfo.name);

        return ko.pureComputed(() => {
            const isLocal = isLocalObservable();
            const link = appUrl.forDocuments(null, dbInfo);
            if (isLocal) {
                return link;
            } else {
                return databases.toExternalUrl(dbInfo, link);
            }
        });
    }

    createAllDocumentsUrlObservableForNode(dbInfo: databaseInfo, node: databaseGroupNode) {
        return ko.pureComputed(() => {
            const currentNodeTag = this.clusterManager.localNodeTag();
            const nodeTag = node.tag();
            const link = appUrl.forDocuments(null, dbInfo);
            if (currentNodeTag === nodeTag) {
                return link;
            } else {
                return appUrl.toExternalUrl(node.serverUrl(), link);
            }
        });
    }

    private static toExternalUrl(dbInfo: databaseInfo, url: string) {
        // we have to redirect to different node, let's find first member where selected database exists
        const firstMember = dbInfo.nodes().find(x => x.type() === "Member");
        const serverUrl = firstMember ? firstMember.serverUrl() : clusterTopologyManager.default.localNodeUrl();
        return appUrl.toExternalUrl(serverUrl, url);
    }

    indexErrorsUrl(dbInfo: databaseInfo): string {
        return appUrl.forIndexErrors(dbInfo);
    }

    storageReportUrl(dbInfo: databaseInfo): string {
        return appUrl.forStatusStorageReport(dbInfo);
    }

    indexesUrl(dbInfo: databaseInfo): string {
        return appUrl.forIndexes(dbInfo);
    } 

    periodicBackupUrl(dbInfo: databaseInfo): string {
        return appUrl.forEditPeriodicBackupTask(dbInfo);
    }

    manageDatabaseGroupUrl(dbInfo: databaseInfo): string {
        return appUrl.forManageDatabaseGroup(dbInfo);
    }

    private getSelectedDatabases() {
        const selected = this.selectedDatabases();
        return this.databases().sortedDatabases().filter(x => _.includes(selected, x.name));
    }

    toggleSelectAll(): void {
        const selectedCount = this.selectedDatabases().length;

        if (selectedCount > 0) {
            this.selectedDatabases([]);
        } else {
            const namesToSelect = [] as Array<string>;

            this.databases().sortedDatabases().forEach(db => {
                if (!db.filteredOut()) {
                    namesToSelect.push(db.name);
                }
            });

            this.selectedDatabases(namesToSelect);
        }
    }

    deleteDatabase(db: databaseInfo) {
        this.deleteDatabases([db]);
    }

    deleteSelectedDatabases() {
       this.deleteDatabases(this.getSelectedDatabases());
    }

    private deleteDatabases(toDelete: databaseInfo[]) {
        const confirmDeleteViewModel = new deleteDatabaseConfirm(toDelete);

        confirmDeleteViewModel
            .result
            .done((confirmResult: deleteDatabaseConfirmResult) => {
                if (confirmResult.can) {   

                    const dbsList = toDelete.map(x => {
                        x.isBeingDeleted(true);
                        const asDatabase = x.asDatabase();

                        // disconnect here to avoid race condition between database deleted message
                        // and websocket disconnection
                        changesContext.default.disconnectIfCurrent(asDatabase, "DatabaseDeleted");
                        return asDatabase;
                    });
                                    
                    new deleteDatabaseCommand(dbsList, !confirmResult.keepFiles)
                        .execute();
                }
            });

        app.showBootstrapDialog(confirmDeleteViewModel);
    }

    private removeDatabase(dbInfo: databaseInfo) {
        this.databases().sortedDatabases.remove(dbInfo);
        this.selectedDatabases.remove(dbInfo.name);
        messagePublisher.reportSuccess(`Database ${dbInfo.name} was successfully deleted`);
    }

    enableSelectedDatabases() {
        this.toggleSelectedDatabases(true);
    }

    disableSelectedDatabases() {
        this.toggleSelectedDatabases(false);
    }

    private toggleSelectedDatabases(enableAll: boolean) {
        const selectedDatabases = this.getSelectedDatabases().map(x => x.asDatabase());

        if (_.every(selectedDatabases, x => x.disabled() !== enableAll)) {
            return;
        }

        if (selectedDatabases.length > 0) {
            const disableDatabaseToggleViewModel = new disableDatabaseToggleConfirm(selectedDatabases, !enableAll);

            disableDatabaseToggleViewModel.result.done(result => {
                if (result.can) {
                    this.spinners.globalToggleDisable(true);

                    new toggleDatabaseCommand(selectedDatabases, !enableAll)
                        .execute()
                        .done(disableResult => {
                            disableResult.Status.forEach(x => this.onDatabaseDisabled(x));
                        })
                        .always(() => this.spinners.globalToggleDisable(false));
                }
            });

            app.showBootstrapDialog(disableDatabaseToggleViewModel);
        }
    }

    toggleDatabase(rsInfo: databaseInfo) {
        const disable = !rsInfo.disabled();

        const rs = rsInfo.asDatabase();
        const disableDatabaseToggleViewModel = new disableDatabaseToggleConfirm([rs], disable);

        disableDatabaseToggleViewModel.result.done(result => {
            if (result.can) {
                rsInfo.inProgressAction(disable ? "Disabling..." : "Enabling...");

                new toggleDatabaseCommand([rs], disable)
                    .execute()
                    .done(disableResult => {
                        disableResult.Status.forEach(x => this.onDatabaseDisabled(x));
                    })
                    .always(() => rsInfo.inProgressAction(null));
            }
        });

        app.showBootstrapDialog(disableDatabaseToggleViewModel);
    }

    private onDatabaseDisabled(result: disableDatabaseResult) {
        const dbs = this.databases().sortedDatabases();
        const matchedDatabase = dbs.find(rs => rs.name === result.Name);

        if (matchedDatabase) {
            matchedDatabase.disabled(result.Disabled);

            // If Enabling a database (that is selected from the top) than we want it to be Online(Loaded)
            if (matchedDatabase.isCurrentlyActiveDatabase() && !matchedDatabase.disabled()) {
                new loadDatabaseCommand(matchedDatabase.asDatabase())
                    .execute();
            }
        }
    }

    toggleDisableDatabaseIndexing(db: databaseInfo) {
        const enableIndexing = db.indexingDisabled();
        const message = enableIndexing ? "Enable" : "Disable";

        this.confirmationMessage("Are you sure?", message + " indexing?")
            .done(result => {
                if (result.can) {
                    db.inProgressAction(enableIndexing ? "Enabling..." : "Disabling...");

                    new toggleDisableIndexingCommand(enableIndexing, db)
                        .execute()
                        .done(() => {
                            db.indexingDisabled(!enableIndexing);
                            db.indexingPaused(false);
                        })
                        .always(() => db.inProgressAction(null));
                }
            });
    }

    togglePauseDatabaseIndexing(db: databaseInfo) {
        const pauseIndexing = db.indexingPaused();
        const message = pauseIndexing ? "Resume" : "Pause";

        this.confirmationMessage("Are you sure?", message + " indexing?")
            .done(result => {
                if (result.can) {
                    db.inProgressAction(pauseIndexing ? "Resuming..." : "Pausing...");

                    new togglePauseIndexingCommand(pauseIndexing, db.asDatabase())
                        .execute()
                        .done(() => db.indexingPaused(!pauseIndexing))
                        .always(() => db.inProgressAction(null));
                }
            });
    }

    newDatabase(isFromBackup: boolean) {
        const createDbView = new createDatabase(isFromBackup);
        app.showBootstrapDialog(createDbView);
    }

    activateDatabase(dbInfo: databaseInfo) {
        const db = this.databasesManager.getDatabaseByName(dbInfo.name);
        if (!db || db.disabled() || !db.relevant())
            return;

        this.databasesManager.activate(db);

        this.updateDatabaseInfo(db.name);
    }

    createIsLocalDatabaseObservable(dbName: string) {
        return ko.pureComputed(() => {
            const nodeTag = this.clusterManager.localNodeTag();
            const dbInfo = this.databases().getByName(dbName);

            const nodeTags = new Set<string>();
            const clusterNodes = dbInfo.nodes();

            // using foreach to register knockout dependencies
            clusterNodes.forEach(n => {
                if (n.type() === "Member" || n.type() === "Promotable") {
                    nodeTags.add(n.tag());
                }
            });
            return nodeTags.has(nodeTag);
        });
    }

    /* TODO: cluster related work

    clusterMode = ko.computed(() => shell.clusterMode());
    developerLicense = ko.computed(() => !license.licenseStatus() || !license.licenseStatus().IsCommercial);
    showCreateCluster = ko.computed(() => !shell.clusterMode());
    canCreateCluster = ko.computed(() => license.licenseStatus() && (!license.licenseStatus().IsCommercial || license.licenseStatus().Attributes.clustering === "true"));
    canNavigateToAdminSettings = ko.computed(() =>
            accessHelper.isGlobalAdmin() || accessHelper.canReadWriteSettings() || accessHelper.canReadSettings());

      navigateToCreateCluster() {
        this.navigate(this.appUrls.adminSettingsCluster());
        shell.disconnectFromResourceChangesApi();
    }
    */
}

export = databases;