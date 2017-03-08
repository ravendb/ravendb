import app = require("durandal/app");
import appUrl = require("common/appUrl");
import viewModelBase = require("viewmodels/viewModelBase");
import accessHelper = require("viewmodels/shell/accessHelper");
import resource = require("models/resources/resource");
import deleteResourceConfirm = require("viewmodels/resources/deleteResourceConfirm");
import createDatabase = require("viewmodels/resources/createDatabase");
import disableResourceToggleConfirm = require("viewmodels/resources/disableResourceToggleConfirm");
import disableResourceToggleCommand = require("commands/resources/disableResourceToggleCommand");
import togglePauseIndexingCommand = require("commands/database/index/togglePauseIndexingCommand");
import toggleDisableIndexingCommand = require("commands/database/index/toggleDisableIndexingCommand");
import deleteResourceCommand = require("commands/resources/deleteResourceCommand");
import loadResourceCommand = require("commands/resources/loadResourceCommand");
import resourcesManager = require("common/shell/resourcesManager");
import changesContext = require("common/changesContext");

import resourcesInfo = require("models/resources/info/resourcesInfo");
import getResourcesCommand = require("commands/resources/getResourcesCommand");
import getResourceCommand = require("commands/resources/getResourceCommand");
import resourceInfo = require("models/resources/info/resourceInfo");
import databaseInfo = require("models/resources/info/databaseInfo");
import database = require("models/resources/database");
import EVENTS = require("common/constants/events");
import messagePublisher = require("common/messagePublisher");

class resources extends viewModelBase {

    resources = ko.observable<resourcesInfo>();

    filters = {
        searchText: ko.observable<string>()
    }

    selectionState: KnockoutComputed<checkbox>;
    selectedResources = ko.observableArray<string>([]);

    spinners = {
        globalToggleDisable: ko.observable<boolean>(false)
    }

    private static compactView = ko.observable<boolean>(false);
    compactView = resources.compactView;

    isGlobalAdmin = accessHelper.isGlobalAdmin;
    
    constructor() {
        super();

        this.bindToCurrentInstance("toggleResource", "togglePauseDatabaseIndexing", "toggleDisableDatabaseIndexing", "deleteResource", "activateResource");

        this.initObservables();
    }

    private initObservables() {
        const filters = this.filters;

        filters.searchText.throttle(200).subscribe(() => this.filterResources());

        this.selectionState = ko.pureComputed<checkbox>(() => {
            const resources = this.resources().sortedResources().filter(x => !x.filteredOut());
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

    activate(args: any): JQueryPromise<Raven.Client.Server.Operations.ResourcesInfo> {
        super.activate(args);

        // we can't use createNotifications here, as it is called after *resource changes API* is connected, but user
        // can enter this view and never select resource

        this.addNotification(this.changesContext.serverNotifications().watchResourceChangeStartingWith("db/", (e: Raven.Server.NotificationCenter.Notifications.Server.ResourceChanged) => this.fetchResource(e)));
        this.addNotification(this.changesContext.serverNotifications().watchReconnect(() => this.fetchResources()));

        // TODO: add notification for fs, cs, ts

        return this.fetchResources();
    }

    attached() {
        super.attached();
        this.updateHelpLink("Z8DC3Q");
        ko.postbox.publish("SetRawJSONUrl", appUrl.forDatabasesRawData());
        this.updateUrl(appUrl.forResources());
    }

    private fetchResources(): JQueryPromise<Raven.Client.Server.Operations.ResourcesInfo> {
        return new getResourcesCommand()
            .execute()
            .done(info => this.resources(new resourcesInfo(info)));
    }

    private fetchResource(e: Raven.Server.NotificationCenter.Notifications.Server.ResourceChanged) {
        const qualiferAndName = resourceInfo.extractQualifierAndNameFromNotification(e.ResourceName);

        switch (e.ChangeType) {
            case "Load":
            case "Put":
                this.updateResourceInfo(qualiferAndName.qualifier, qualiferAndName.name);
                break;

            case "Delete":
                const resource = this.resources().sortedResources().find(rs => rs.qualifiedName === e.ResourceName);
                if (resource) {
                    this.removeResource(resource);
                }
                break;
        }
    }

    private updateResourceInfo(qualifer: string, resourceName: string) {
        new getResourceCommand(qualifer, resourceName)
            .execute()
            .done((result: Raven.Client.Server.Operations.ResourceInfo) => {
                this.resources().updateResource(result, qualifer);
                this.filterResources();
            });
    }

    private filterResources(): void {
        const filters = this.filters;
        let searchText = filters.searchText();
        const hasSearchText = !!searchText;

        if (hasSearchText) {
            searchText = searchText.toLowerCase();
        }

        const matchesFilters = (rs: resourceInfo) => !hasSearchText || rs.name.toLowerCase().indexOf(searchText) >= 0;

        const resources = this.resources();
        resources.sortedResources().forEach(resource => {
            const matches = matchesFilters(resource);
            resource.filteredOut(!matches);

            if (!matches) {
                this.selectedResources.remove(resource.qualifiedName);
            }
        });
    }

    resourceUrl(rs: resourceInfo): string {
        if (rs instanceof databaseInfo) {
            const db = rs.asResource();
            return appUrl.forDocuments(null, db);
        }
        //TODO:fs, cs, ts

        return null;
    }

    private getSelectedResources() {
        const selected = this.selectedResources();
        return this.resources().sortedResources().filter(x => _.includes(selected, x.qualifiedName));
    }

    toggleSelectAll(): void {
        const selectedCount = this.selectedResources().length;

        if (selectedCount > 0) {
            this.selectedResources([]);
        } else {
            const namesToSelect = [] as Array<string>;

            this.resources().sortedResources().forEach(resource => {
                if (!resource.filteredOut()) {
                    namesToSelect.push(resource.qualifiedName);
                }
            });

            this.selectedResources(namesToSelect);
        }
    }

    deleteResource(rs: resourceInfo) {
        this.deleteResources([rs]);
    }

    deleteSelectedResources() {
       this.deleteResources(this.getSelectedResources());
    }

    private deleteResources(toDelete: resourceInfo[]) {
        const confirmDeleteViewModel = new deleteResourceConfirm(toDelete);

        confirmDeleteViewModel
            .result
            .done((confirmResult: deleteResourceConfirmResult) => {
                if (confirmResult.can) {   

                    const resourcesList = toDelete.map(x => {
                        x.isBeingDeleted(true);
                        const asResource = x.asResource();

                        // disconnect here to avoid race condition between resource deleted message
                        // and websocket disconnection
                        changesContext.default.disconnectIfCurrent(asResource, "ResourceDeleted");
                        return asResource;
                    });
                                    
                    new deleteResourceCommand(resourcesList, !confirmResult.keepFiles)
                                             .execute()                                            
                                             .done((deletedResources: Array<Raven.Server.Web.System.ResourceDeleteResult>) => {
                                                    deletedResources.forEach(rs => this.onResourceDeleted(rs));                            
                                              });
                }
            });

        app.showBootstrapDialog(confirmDeleteViewModel);
    }

    private onResourceDeleted(deletedResourceResult: Raven.Server.Web.System.ResourceDeleteResult) {
        const matchedResource = this.resources()
            .sortedResources()           
            .find(x => x.qualifiedName.toLowerCase() === deletedResourceResult.QualifiedName.toLowerCase());

        // Resources will be removed from the the sortedResources in method removeResource through the global changes api flow..
        // So only enable the 'delete' button and display err msg if relevant                                
        if (matchedResource && (deletedResourceResult.Reason)) {                           
                matchedResource.isBeingDeleted(false);
                messagePublisher.reportError(`Failed to delete ${matchedResource.name}, reason: ${deletedResourceResult.Reason}`);
        }        
    }

    private removeResource(rsInfo: resourceInfo) {
        this.resources().sortedResources.remove(rsInfo);
        this.selectedResources.remove(rsInfo.qualifiedName);
        messagePublisher.reportSuccess(`Resource ${rsInfo.name} was successfully deleted`);
    }

    enableSelectedResources() {
        this.toggleSelectedResources(true);
    }

    disableSelectedResources() {
        this.toggleSelectedResources(false);
    }

    private toggleSelectedResources(enableAll: boolean) { 
        const selectedResources = this.getSelectedResources().map(x => x.asResource());

        if (_.every(selectedResources, x => x.disabled() !== enableAll)) {
            return;
        }

        if (selectedResources.length > 0) {
            const disableDatabaseToggleViewModel = new disableResourceToggleConfirm(selectedResources, !enableAll);

            disableDatabaseToggleViewModel.result.done(result => {
                if (result.can) {
                    this.spinners.globalToggleDisable(true);

                    new disableResourceToggleCommand(selectedResources, !enableAll)
                        .execute()
                        .done(disableResult => {
                            disableResult.forEach(x => this.onResourceDisabled(x));
                        })
                        .always(() => this.spinners.globalToggleDisable(false));
                }
            });

            app.showBootstrapDialog(disableDatabaseToggleViewModel);
        }
    }

    toggleResource(rsInfo: resourceInfo) {
        const disable = !rsInfo.disabled();

        const rs = rsInfo.asResource();
        const disableDatabaseToggleViewModel = new disableResourceToggleConfirm([rs], disable);

        disableDatabaseToggleViewModel.result.done(result => {
            if (result.can) {
                rsInfo.inProgressAction(disable ? "Disabling..." : "Enabling...");

                new disableResourceToggleCommand([rs], disable)
                    .execute()
                    .done(disableResult => {
                        disableResult.forEach(x => this.onResourceDisabled(x));
                    })
                    .always(() => rsInfo.inProgressAction(null));
            }
        });

        app.showBootstrapDialog(disableDatabaseToggleViewModel);
    }

    private onResourceDisabled(result: disableResourceResult) {
        const resources = this.resources().sortedResources();
        const matchedResource = resources.find(rs => rs.qualifiedName === result.QualifiedName);

        if (matchedResource) {
            matchedResource.disabled(result.Disabled);

            // If Enabling a resource (that is selected from the top) than we want it to be Online(Loaded)
            if (matchedResource.isCurrentlyActiveResource() && !matchedResource.disabled()) {
                new loadResourceCommand(matchedResource.asResource())
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

                    new togglePauseIndexingCommand(pauseIndexing, db.asResource())
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

    activateResource(rsInfo: resourceInfo) {
        let resource = this.resourcesManager.getResourceByQualifiedName(rsInfo.qualifiedName);
        if (!resource || resource.disabled())
            return;

        resource.activate();

        this.updateResourceInfo(resource.qualifier, resource.name);
    }

    createNewResource() {
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

export = resources;



