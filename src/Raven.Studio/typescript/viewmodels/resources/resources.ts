import app = require("durandal/app");
import appUrl = require("common/appUrl");
import viewModelBase = require("viewmodels/viewModelBase");
import accessHelper = require("viewmodels/shell/accessHelper");
import resource = require("models/resources/resource");
import deleteResourceConfirm = require("viewmodels/resources/deleteResourceConfirm");
import createDatabase = require("viewmodels/resources/createDatabase");
import disableResourceToggleConfirm = require("viewmodels/resources/disableResourceToggleConfirm");
import disableResourceToggleCommand = require("commands/resources/disableResourceToggleCommand");
import toggleIndexingCommand = require("commands/database/index/toggleIndexingCommand");
import deleteResourceCommand = require("commands/resources/deleteResourceCommand");
import loadResourceCommand = require("commands/resources/loadResourceCommand");
import resourcesManager = require("common/shell/resourcesManager");

import resourcesInfo = require("models/resources/info/resourcesInfo");
import getResourcesCommand = require("commands/resources/getResourcesCommand");
import getResourceCommand = require("commands/resources/getResourceCommand");
import resourceInfo = require("models/resources/info/resourceInfo");
import databaseInfo = require("models/resources/info/databaseInfo");
import filesystemInfo = require("models/resources/info/filesystemInfo");
import database = require("models/resources/database");
import EVENTS = require("common/constants/events");

class resources extends viewModelBase {

    resources = ko.observable<resourcesInfo>();

    filters = {
        searchText: ko.observable<string>(),
        includeDatabases: ko.observable<boolean>(true),
        includeFileSystems: ko.observable<boolean>(true)
    }

    selectionState: KnockoutComputed<checkbox>;
    selectedResources = ko.observableArray<string>([]);
    allCheckedResourcesDisabled: KnockoutComputed<boolean>;

    spinners = {
        globalToggleDisable: ko.observable<boolean>(false),
        itemTakedowns: ko.observableArray<string>([]),
        disableIndexing: ko.observableArray<string>([]), //TODO: bind on UI
        toggleRejectMode: ko.observableArray<string>([]) //TODO: bind on UI
    }

    private static compactView = ko.observable<boolean>(false);
    compactView = resources.compactView;

    isGlobalAdmin = accessHelper.isGlobalAdmin;
    
    constructor() {
        super();

        this.bindToCurrentInstance("toggleResource", "toggleDatabaseIndexing", "deleteResource", "activateResource");

        this.initObservables();
    }

    private initObservables() {
        const filters = this.filters;

        filters.searchText.throttle(200).subscribe(() => this.filterResources());
        filters.includeDatabases.subscribe(() => this.filterResources());
        filters.includeFileSystems.subscribe(() => this.filterResources());

        this.selectionState = ko.pureComputed<checkbox>(() => {
            const resources = this.resources().sortedResources().filter(x => !x.filteredOut());
            var selectedCount = this.selectedResources().length;
            if (resources.length && selectedCount === resources.length)
                return checkbox.Checked;
            if (selectedCount > 0)
                return checkbox.SomeChecked;
            return checkbox.UnChecked;
        });

        this.allCheckedResourcesDisabled = ko.pureComputed(() => {
            const selected = this.getSelectedResources();
            return selected.length === selected.filter(x => x.disabled()).length;
        });
    }

    // Override canActivate: we can always load this page, regardless of any system db prompt.
    canActivate(args: any): any {
        return true;
    }

    activate(args: any): JQueryPromise<Raven.Client.Data.ResourcesInfo> {
        super.activate(args);

        // we can't use createNotifications here, as it is called after *resource changes API* is connected, but user
        // can enter this view and never select resource
        this.addNotification(this.changesContext.globalChangesApi().watchItemsStartingWith("db/", (e: Raven.Server.Alerts.GlobalAlertNotification) => this.fetchResource(e)));
        this.addNotification(this.changesContext.globalChangesApi().watchReconnect(() => this.fetchResources()));

        // TODO: add notification for fs, cs, ts

        return this.fetchResources();
    }

    attached() {
        super.attached();
        this.updateHelpLink("Z8DC3Q");
        ko.postbox.publish("SetRawJSONUrl", appUrl.forDatabasesRawData());
    }

    private fetchResources(): JQueryPromise<Raven.Client.Data.ResourcesInfo> {
        return new getResourcesCommand()
            .execute()
            .done(info => this.resources(new resourcesInfo(info)));
    }

    private fetchResource(e: Raven.Server.Alerts.GlobalAlertNotification) {
        const qualiferAndName = resourceInfo.extractQualifierAndNameFromNotification(e.Id);

        switch (e.Operation) {
            case "Loaded":
            case "Write":
                this.updateResourceInfo(qualiferAndName.qualifier, qualiferAndName.name);
                break;

            case "Delete":
                let resource = this.resources().sortedResources().find(rs => rs.qualifiedName === e.Id);
                if (resource) {
                    this.removeResource(resource);
                }
                break;
        }
    }

    private updateResourceInfo(qualifer: string, resourceName: string) {
        new getResourceCommand(qualifer, resourceName)
            .execute()
            .done((result: Raven.Client.Data.ResourceInfo) => {
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

        const matchesFilters = (rs: resourceInfo) => {
            if (rs instanceof databaseInfo && !filters.includeDatabases())
                return false;

            if (rs instanceof filesystemInfo && !filters.includeFileSystems())
                return false;

            return !hasSearchText || rs.name.toLowerCase().indexOf(searchText) >= 0;
        }

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
                    new deleteResourceCommand(toDelete.map(x => x.asResource()), !confirmResult.keepFiles)
                        .execute()
                        .done((deletedResources: Array<resource>) => {
                            deletedResources.forEach(rs => this.onResourceDeleted(rs));
                        });
                }
            });

        app.showBootstrapDialog(confirmDeleteViewModel);
    }

    private onResourceDeleted(deletedResource: resource) {
        const matchedResource = this.resources()
            .sortedResources()
            .find(x => x.qualifiedName.toLowerCase() === deletedResource.qualifiedName.toLowerCase());

        if (matchedResource) {
            this.removeResource(matchedResource);
        }
    }

    private removeResource(rsInfo: resourceInfo) {
        this.resources().sortedResources.remove(rsInfo);
        this.selectedResources.remove(rsInfo.qualifiedName);
    }

    toggleSelectedResources() {
        const disableAll = !this.allCheckedResourcesDisabled();
        const selectedResources = this.getSelectedResources().map(x => x.asResource());

        if (selectedResources.length > 0) {
            const disableDatabaseToggleViewModel = new disableResourceToggleConfirm(selectedResources, disableAll);

            disableDatabaseToggleViewModel.result.done(result => {
                if (result.can) {
                    this.spinners.globalToggleDisable(true);

                    new disableResourceToggleCommand(selectedResources, disableAll)
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
                this.spinners.itemTakedowns.push(rs.qualifiedName);

                new disableResourceToggleCommand([rs], disable)
                    .execute()
                    .done(disableResult => {
                        disableResult.forEach(x => this.onResourceDisabled(x));
                    })
                    .always(() => this.spinners.itemTakedowns.remove(rs.qualifiedName));
            }
        });

        app.showBootstrapDialog(disableDatabaseToggleViewModel);
    }

    private onResourceDisabled(result: disableResourceResult) {
        const resources = this.resources().sortedResources();
        const matchedResource = resources.find(rs => rs.qualifiedName === result.qualifiedName);

        if (matchedResource) {
            matchedResource.disabled(result.disabled);

            // If Enabling a resource (that is selected from the top) than we want it to be Online(Loaded)
            if (matchedResource.isCurrentlyActiveResource() && !matchedResource.disabled()) {
                new loadResourceCommand(matchedResource.asResource())
                    .execute();
            }
        }
    }

    toggleDatabaseIndexing(db: databaseInfo) {
        const enableIndexing = !db.indexingEnabled();
        const message = enableIndexing ? "Enable" : "Disable";

        this.confirmationMessage("Are you sure?", message + " indexing?")
            .done(result => {
                if (result.can) {
                    this.spinners.disableIndexing.push(db.qualifiedName);

                    new toggleIndexingCommand(true, db.asResource())
                        .execute()
                        .done(() => db.indexingEnabled(enableIndexing))
                        .always(() => this.spinners.disableIndexing.remove(db.qualifiedName));
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



