import app = require("durandal/app");
import appUrl = require("common/appUrl");
import viewModelBase = require("viewmodels/viewModelBase");
import accessHelper = require("viewmodels/shell/accessHelper");
import EVENTS = require("common/constants/events");

import resource = require("models/resources/resource");
import database = require("models/resources/database");

import deleteResourceConfirm = require("viewmodels/resources/deleteResourceConfirm");
import disableResourceToggleConfirm = require("viewmodels/resources/disableResourceToggleConfirm");
import toggleRejectDatabaseClients = require("commands/maintenance/toggleRejectDatabaseClients");
import disableResourceToggleCommand = require("commands/resources/disableResourceToggleCommand");
import toggleIndexingCommand = require("commands/database/index/toggleIndexingCommand");

import resourcesInfo = require("models/resources/info/resourcesInfo");
import getResourcesCommand = require("commands/resources/getResourcesCommand");
import resourceInfo = require("models/resources/info/resourceInfo");
import databaseInfo = require("models/resources/info/databaseInfo");
import filesystemInfo = require("models/resources/info/filesystemInfo");

class resources extends viewModelBase {

    resources = ko.observable<resourcesInfo>();

    filters = {
        searchText: ko.observable<string>(),
        includeDatabases: ko.observable<boolean>(true),
        includeFilesystems: ko.observable<boolean>(true)
    }

    selectionState: KnockoutComputed<checkbox>;
    selectedResources = ko.observableArray<string>([]);
    allCheckedResourcesDisabled: KnockoutComputed<boolean>;

    static compactView = ko.observable<boolean>(false);
    compactView = resources.compactView;

    isGlobalAdmin = accessHelper.isGlobalAdmin;
    
    constructor() {
        super();
        this.initObservables();
    }

    private initObservables() {
        const filters = this.filters;

        filters.searchText.throttle(200).subscribe(() => this.filterResources());
        filters.includeDatabases.subscribe(() => this.filterResources());
        filters.includeFilesystems.subscribe(() => this.filterResources());

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

    createPostboxSubscriptions(): KnockoutSubscription[] {
        return [
            ko.postbox.subscribe(EVENTS.Resource.Created,
                (value: resourceCreatedEventArgs) => {
                    //TODO: we are assuming it is database for now. 
                    this.fetchResources();
                })
        ];
    }

    // Override canActivate: we can always load this page, regardless of any system db prompt.
    canActivate(args: any): any {
        return true;
    }

    activate(args: any): JQueryPromise<resourcesInfo> {
        super.activate(args);
        return this.fetchResources();
    }

    private fetchResources(): JQueryPromise<resourcesInfo> {
        return new getResourcesCommand()
            .execute()
            .done(info => this.resources(info));
    }

    attached() {
        super.attached();
        this.updateHelpLink("Z8DC3Q");
        ko.postbox.publish("SetRawJSONUrl", appUrl.forDatabasesRawData());
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

            if (rs instanceof filesystemInfo && !filters.includeFilesystems())
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
        return this.resources().sortedResources().filter(x => selected.contains(x.qualifiedName));
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

    deleteSelectedResources() {
        const selectedResources = this.getSelectedResources();
        const confirmDeleteViewModel = new deleteResourceConfirm(selectedResources);

        confirmDeleteViewModel
            .deleteTask
            .done((deletedResources: Array<resource>) => {
                deletedResources.forEach(rs => this.onResourceDeleted(rs));
            });

        app.showDialog(confirmDeleteViewModel);
    }

    private onResourceDeleted(deletedResource: resource) {
        const matchedResource = this.resources().sortedResources().find(x => x.qualifiedName === deletedResource.qualifiedName);

        if (matchedResource) {
            this.resources().sortedResources.remove(matchedResource);
            this.selectedResources.remove(matchedResource.qualifiedName);

            this.changesContext.disconnectIfCurrent(matchedResource.asResource());
        }
    }

    toggleSelectedResources() {
        const disableAll = !this.allCheckedResourcesDisabled();
        const selectedResources = this.getSelectedResources().map(x => x.asResource());

        if (selectedResources.length > 0) {
            const disableDatabaseToggleViewModel = new disableResourceToggleConfirm(selectedResources, disableAll);

            disableDatabaseToggleViewModel.result.done(result => {
                if (result.can) {
                    if (disableAll) {
                        selectedResources.forEach(rs => {
                            this.changesContext.disconnectIfCurrent(rs);        
                        });
                    }

                    //TODO: spinners

                    new disableResourceToggleCommand(selectedResources, disableAll)
                        .execute()
                        .done(disableResult => {
                            //TODO: update UI state via onResourceDisabledToggle
                        });
                }
            });

            app.showDialog(disableDatabaseToggleViewModel);
        }
    }

    private onResourceDisabledToggle(rs: resource, action: boolean) { //TODO: should we operate on resource or resourceInfo here?
        //TODO: review body of this method
        if (rs) {
            /* TODO:
            rs.disabled(action);
            //TODO: rs.isChecked(false);

            if (!rs.disabled() === false) {
                rs.activate();
            }*/
        }
    }

    toggleDatabaseIndexing(db: databaseInfo) {
        /* TODO:
        const start = db.indexingDisabled();
        const actionText = db.indexingDisabled() ? "Enable" : "Disable";
        const message = this.confirmationMessage(actionText + " indexing?", "Are you sure?");

        message.done(result => {
            if (result.can) {
                new toggleIndexingCommand(start, db.asResource())
                    .execute(); //TODO: update spinner + UI
            }
        });*/
    }

    toggleRejectDatabaseClients(db: databaseInfo) {
        /* TODO
        var action = !db.rejectClientsMode();
        var actionText = action ? "reject clients mode" : "accept clients mode";
        var message = this.confirmationMessage("Switch to " + actionText, "Are you sure?");
        message.done(() => {
            var task = new toggleRejectDatabaseClients(db.name, action).execute();
            task.done(() => db.rejectClientsMode(action));
        });*/
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



