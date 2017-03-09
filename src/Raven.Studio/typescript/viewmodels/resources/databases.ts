import app = require("durandal/app");
import appUrl = require("common/appUrl");
import viewModelBase = require("viewmodels/viewModelBase");
import accessHelper = require("viewmodels/shell/accessHelper");
import deleteDatabaseConfirm = require("viewmodels/resources/deleteDatabaseConfirm");
import createDatabase = require("viewmodels/resources/createDatabase");
import disableDatabaseToggleConfirm = require("viewmodels/resources/disableDatabaseToggleConfirm");
import disableDatabaseToggleCommand = require("commands/resources/disableDatabaseToggleCommand");
import togglePauseIndexingCommand = require("commands/database/index/togglePauseIndexingCommand");
import toggleDisableIndexingCommand = require("commands/database/index/toggleDisableIndexingCommand");
import deleteDatabaseCommand = require("commands/resources/deleteDatabaseCommand");
import loadDatabaseCommand = require("commands/resources/loadDatabaseCommand");
import resourcesManager = require("common/shell/resourcesManager");
import changesContext = require("common/changesContext");

import databasesInfo = require("models/resources/info/databasesInfo");
import getDatabasesCommand = require("commands/resources/getDatabasesCommand");
import getDatabaseCommand = require("commands/resources/getDatabaseCommand");
import databaseInfo = require("models/resources/info/databaseInfo");
import database = require("models/resources/database");
import EVENTS = require("common/constants/events");
import messagePublisher = require("common/messagePublisher");

class databases extends viewModelBase {

    databases = ko.observable<databasesInfo>();

    filters = {
        searchText: ko.observable<string>()
    }

    selectionState: KnockoutComputed<checkbox>;
    selectedResources = ko.observableArray<string>([]);

    spinners = {
        globalToggleDisable: ko.observable<boolean>(false)
    }

    private static compactView = ko.observable<boolean>(false);
    compactView = databases.compactView;

    isGlobalAdmin = accessHelper.isGlobalAdmin;
    
    constructor() {
        super();

        this.bindToCurrentInstance("toggleDatabase", "togglePauseDatabaseIndexing", "toggleDisableDatabaseIndexing", "deleteDatabase", "activateDatabase");

        this.initObservables();
    }

    private initObservables() {
        const filters = this.filters;

        filters.searchText.throttle(200).subscribe(() => this.filterDatabases());

        this.selectionState = ko.pureComputed<checkbox>(() => {
            const resources = this.databases().sortedDatabases().filter(x => !x.filteredOut());
            var selectedCount = this.selectedResources().length;
            if (resources.length && selectedCount === resources.length)
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

        this.addNotification(this.changesContext.serverNotifications().watchDatabaseChangeStartingWith("db/", (e: Raven.Server.NotificationCenter.Notifications.Server.DatabaseChanged) => this.fetchDatabase(e)));
        this.addNotification(this.changesContext.serverNotifications().watchReconnect(() => this.fetchDatabases()));

        return this.fetchDatabases();
    }

    attached() {
        super.attached();
        this.updateHelpLink("Z8DC3Q");
        ko.postbox.publish("SetRawJSONUrl", appUrl.forDatabasesRawData());
        this.updateUrl(appUrl.forDatabases());
    }

    private fetchDatabases(): JQueryPromise<Raven.Client.Server.Operations.DatabasesInfo> {
        return new getDatabasesCommand()
            .execute()
            .done(info => this.databases(new databasesInfo(info)));
    }

    private fetchDatabase(e: Raven.Server.NotificationCenter.Notifications.Server.DatabaseChanged) {
        const qualiferAndName = databaseInfo.extractQualifierAndNameFromNotification(e.DatabaseName);

        switch (e.ChangeType) {
            case "Load":
            case "Put":
                this.updateDatabaseInfo(qualiferAndName.qualifier, qualiferAndName.name);
                break;

            case "Delete":
                const db = this.databases().sortedDatabases().find(rs => rs.qualifiedName === e.DatabaseName);
                if (db) {
                    this.removeDatabase(db);
                }
                break;
        }
    }

    private updateDatabaseInfo(qualifer: string, resourceName: string) {
        new getDatabaseCommand(resourceName)
            .execute()
            .done((result: Raven.Client.Server.Operations.DatabaseInfo) => {
                this.databases().updateDatabase(result, qualifer);
                this.filterDatabases();
            });
    }

    private filterDatabases(): void {
        const filters = this.filters;
        let searchText = filters.searchText();
        const hasSearchText = !!searchText;

        if (hasSearchText) {
            searchText = searchText.toLowerCase();
        }

        const matchesFilters = (rs: databaseInfo) => !hasSearchText || rs.name.toLowerCase().indexOf(searchText) >= 0;

        const resources = this.databases();
        resources.sortedDatabases().forEach(resource => {
            const matches = matchesFilters(resource);
            resource.filteredOut(!matches);

            if (!matches) {
                this.selectedResources.remove(resource.qualifiedName);
            }
        });
    }

    resourceUrl(dbInfo: databaseInfo): string {
        const db = dbInfo.asDatabase();
        return appUrl.forDocuments(null, db);
    }

    private getSelectedDatabases() {
        const selected = this.selectedResources();
        return this.databases().sortedDatabases().filter(x => _.includes(selected, x.qualifiedName));
    }

    toggleSelectAll(): void {
        const selectedCount = this.selectedResources().length;

        if (selectedCount > 0) {
            this.selectedResources([]);
        } else {
            const namesToSelect = [] as Array<string>;

            this.databases().sortedDatabases().forEach(resource => {
                if (!resource.filteredOut()) {
                    namesToSelect.push(resource.qualifiedName);
                }
            });

            this.selectedResources(namesToSelect);
        }
    }

    deleteDatabase(db: databaseInfo) {
        this.deleteResources([db]);
    }

    deleteSelectedDatabases() {
       this.deleteResources(this.getSelectedDatabases());
    }

    private deleteResources(toDelete: databaseInfo[]) {
        const confirmDeleteViewModel = new deleteDatabaseConfirm(toDelete);

        confirmDeleteViewModel
            .result
            .done((confirmResult: deleteDatabaseConfirmResult) => {
                if (confirmResult.can) {   

                    const resourcesList = toDelete.map(x => {
                        x.isBeingDeleted(true);
                        const asDatabase = x.asDatabase();

                        // disconnect here to avoid race condition between resource deleted message
                        // and websocket disconnection
                        changesContext.default.disconnectIfCurrent(asDatabase, "DatabaseDeleted");
                        return asDatabase;
                    });
                                    
                    new deleteDatabaseCommand(resourcesList, !confirmResult.keepFiles)
                                             .execute()                                            
                                             .done((deletedResources: Array<Raven.Server.Web.System.ResourceDeleteResult>) => {
                                                    deletedResources.forEach(rs => this.onResourceDeleted(rs));                            
                                              });
                }
            });

        app.showBootstrapDialog(confirmDeleteViewModel);
    }

    private onResourceDeleted(deletedResourceResult: Raven.Server.Web.System.ResourceDeleteResult) {
        const matchedResource = this.databases()
            .sortedDatabases()           
            .find(x => x.qualifiedName.toLowerCase() === deletedResourceResult.QualifiedName.toLowerCase());

        // Resources will be removed from the the sortedResources in method removeResource through the global changes api flow..
        // So only enable the 'delete' button and display err msg if relevant                                
        if (matchedResource && (deletedResourceResult.Reason)) {                           
                matchedResource.isBeingDeleted(false);
                messagePublisher.reportError(`Failed to delete ${matchedResource.name}, reason: ${deletedResourceResult.Reason}`);
        }        
    }

    private removeDatabase(rsInfo: databaseInfo) {
        this.databases().sortedDatabases.remove(rsInfo);
        this.selectedResources.remove(rsInfo.qualifiedName);
        messagePublisher.reportSuccess(`Resource ${rsInfo.name} was successfully deleted`);
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

                    new disableDatabaseToggleCommand(selectedDatabases, !enableAll)
                        .execute()
                        .done(disableResult => {
                            disableResult.forEach(x => this.onDatabaseDisabled(x));
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

                new disableDatabaseToggleCommand([rs], disable)
                    .execute()
                    .done(disableResult => {
                        disableResult.forEach(x => this.onDatabaseDisabled(x));
                    })
                    .always(() => rsInfo.inProgressAction(null));
            }
        });

        app.showBootstrapDialog(disableDatabaseToggleViewModel);
    }

    private onDatabaseDisabled(result: disableDatabaseResult) {
        const dbs = this.databases().sortedDatabases();
        const matchedDatabase = dbs.find(rs => rs.qualifiedName === result.QualifiedName);

        if (matchedDatabase) {
            matchedDatabase.disabled(result.Disabled);

            // If Enabling a resource (that is selected from the top) than we want it to be Online(Loaded)
            if (matchedDatabase.isCurrentlyActiveResource() && !matchedDatabase.disabled()) {
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

    toggleRejectDatabaseClients(db: databaseInfo) {
        const rejectClients = !db.rejectClients();

        const message = rejectClients ? "reject clients mode" : "accept clients mode";
        this.confirmationMessage("Are you sure?", "Switch to " + message)
            .done(result => {
                if (result.can) {
                    //TODO: progress (this.spinners.toggleRejectMode), command, update db object, etc
                }
            });
    }

    newDatabase() {
        const createDbView = new createDatabase();
        app.showBootstrapDialog(createDbView);
    }

    activateDatabase(dbInfo: databaseInfo) {
        let db = this.resourcesManager.getResourceByQualifiedName(dbInfo.qualifiedName);
        if (!db || db.disabled())
            return;

        db.activate();

        this.updateDatabaseInfo(db.qualifier, db.name);
    }

    createNewDatabase() {
        this.newDatabase();
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