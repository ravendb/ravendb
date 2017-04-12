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
import changesContext = require("common/changesContext");

import databasesInfo = require("models/resources/info/databasesInfo");
import getDatabasesCommand = require("commands/resources/getDatabasesCommand");
import getDatabaseCommand = require("commands/resources/getDatabaseCommand");
import databaseInfo = require("models/resources/info/databaseInfo");
import messagePublisher = require("common/messagePublisher");

class databases extends viewModelBase {

    databases = ko.observable<databasesInfo>();

    filters = {
        searchText: ko.observable<string>()
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

        this.bindToCurrentInstance("toggleDatabase", "togglePauseDatabaseIndexing", "toggleDisableDatabaseIndexing", "deleteDatabase", "activateDatabase");

        this.initObservables();
    }

    private initObservables() {
        const filters = this.filters;

        filters.searchText.throttle(200).subscribe(() => this.filterDatabases());

        this.selectionState = ko.pureComputed<checkbox>(() => {
            const databases = this.databases().sortedDatabases().filter(x => !x.filteredOut());
            var selectedCount = this.selectedDatabases().length;
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

        this.addNotification(this.changesContext.serverNotifications().watchAllDatabaseChanges((e: Raven.Server.NotificationCenter.Notifications.Server.DatabaseChanged) => this.fetchDatabase(e)));
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
        switch (e.ChangeType) {
            case "Load":
            case "Put":
                this.updateDatabaseInfo(e.DatabaseName);
                break;

            case "Delete":
                const db = this.databases().sortedDatabases().find(rs => rs.name === e.DatabaseName);
                if (db) {
                    this.removeDatabase(db);
                }
                break;
        }
    }

    private updateDatabaseInfo(databaseName: string) {
        new getDatabaseCommand(databaseName)
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

        if (hasSearchText) {
            searchText = searchText.toLowerCase();
        }

        const matchesFilters = (rs: databaseInfo) => !hasSearchText || rs.name.toLowerCase().indexOf(searchText) >= 0;

        const databases = this.databases();
        databases.sortedDatabases().forEach(db => {
            const matches = matchesFilters(db);
            db.filteredOut(!matches);

            if (!matches) {
                this.selectedDatabases.remove(db.name);
            }
        });
    }

    databaseUrl(dbInfo: databaseInfo): string {
        const db = dbInfo.asDatabase();
        return appUrl.forDocuments(null, db);
    }

    indexErrorsUrl(dbInfo: databaseInfo): string {
        const db = dbInfo.asDatabase();
        return appUrl.forIndexErrors(db);
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
                                             .execute()                                            
                                             .done((deletedDatabases: Array<Raven.Server.Web.System.DatabaseDeleteResult>) => {
                                                    deletedDatabases.forEach(rs => this.onDatabaseDeleted(rs));                            
                                              });
                }
            });

        app.showBootstrapDialog(confirmDeleteViewModel);
    }

    private onDatabaseDeleted(deletedDatabaseResult: Raven.Server.Web.System.DatabaseDeleteResult) {
        const matchedDatabase = this.databases()
            .sortedDatabases()
            .find(x => x.name.toLowerCase() === deletedDatabaseResult.Name.toLowerCase());

        // Databases will be removed from the the sortedDatabases in method removeDatabase through the global changes api flow..
        // So only enable the 'delete' button and display err msg if relevant                                
        if (matchedDatabase && (deletedDatabaseResult.Reason)) {                           
                matchedDatabase.isBeingDeleted(false);
                messagePublisher.reportError(`Failed to delete ${matchedDatabase.name}, reason: ${deletedDatabaseResult.Reason}`);
        }        
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
        let db = this.databasesManager.getDatabaseByName(dbInfo.name);
        if (!db || db.disabled())
            return;

        this.databasesManager.activate(db);

        this.updateDatabaseInfo(db.name);
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