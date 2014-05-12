import app = require("durandal/app");
import system = require("durandal/system");
import durandalRouter = require("plugins/router");
import appUrl = require("common/appUrl");

import viewModelBase = require("viewmodels/viewModelBase");

import synchronizationDetails = require("models/filesystem/synchronizationDetails");
import synchronizationReport = require("models/filesystem/synchronizationReport");
import synchronizationDetail = require("models/filesystem/synchronizationDetail");
import synchronizationDestination = require("models/filesystem/synchronizationDestination");
import pagedList = require("common/pagedList");
import pagedResultSet = require("common/pagedResultSet");

import getDestinationsCommand = require("commands/filesystem/getDestinationsCommand");
import getFilesConflictsCommand = require("commands/filesystem/getFilesConflictsCommand");
import saveDestinationCommand = require("commands/filesystem/saveDestinationCommand");
import deleteDestinationCommand = require("commands/filesystem/deleteDestinationCommand");
import synchronizeWithDestinationCommand = require("commands/filesystem/synchronizeWithDestinationCommand");
import resolveConflictCommand = require("commands/filesystem/resolveConflictCommand");
import virtualTable = require("widgets/virtualTable/viewModel");

import filesystemAddDestination = require("viewmodels/filesystem/filesystemAddDestination");
import resolveConflict = require("viewmodels/filesystem/resolveConflict");
import filesystem = require("models/filesystem/filesystem");

class synchronization extends viewModelBase {

    destinations = ko.observableArray<synchronizationDestinationDto>();
    isDestinationsVisible = ko.computed(() => this.destinations().length > 0);

    conflicts = ko.observableArray<string>();
    selectedConflicts = ko.observableArray<string>();
    isConflictsVisible = ko.computed(() => this.conflicts().length > 0);
          
    private pollingInterval: any;
    private activeFilesystemSubscription: any;
    synchronizationUrl = appUrl.forCurrentDatabase().filesystemSynchronization;

    router: DurandalRootRouter;
    static statusRouter: DurandalRouter; //TODO: is it better way of exposing this router to child router?
    currentRouteTitle: KnockoutComputed<string>;
    appUrls: computedAppUrls;

    constructor() {
        super();

        this.appUrls = appUrl.forCurrentFilesystem();

        this.router = durandalRouter.createChildRouter()
            .map([
                { route: 'filesystems/synchronization', moduleId: 'viewmodels/filesystem/synchronizationActivity', title: 'Activity', nav: true, hash: appUrl.forCurrentFilesystem().filesystemSynchronization },
                { route: 'filesystems/synchronization/conflicts', moduleId: 'viewmodels/filesystem/synchronizationConflicts', title: 'Conflicts', nav: true, hash: appUrl.forCurrentFilesystem().filesystemSynchronizationConflicts },
                { route: 'filesystems/synchronization/destinations', moduleId: 'viewmodels/filesystem/synchronizationDestinations', title: 'Destinations', nav: true, hash: appUrl.forCurrentFilesystem().filesystemSynchronizationDestinations }
            ])
            .buildNavigationModel();

        synchronization.statusRouter = this.router;

        this.currentRouteTitle = ko.computed(() => {
            // Is there a better way to get the active route?
            var activeRoute = this.router.navigationModel().first(r => r.isActive());
            return activeRoute != null ? activeRoute.title : "";
        });
    }

    activate(args) {
        super.activate(args);
        this.activeFilesystemSubscription = this.activeFilesystem.subscribe((fs: filesystem) => this.fileSystemChanged(fs));
    }

    deactivate() {
        super.deactivate();
        this.activeFilesystemSubscription.dispose();
    }

    forceModelPolling() {
    }

    addDestination() {
        //var fs = this.activeFilesystem();
        //require(["viewmodels/filesystem/filesystemAddDestination"], filesystemAddDestination => {
        //    var addDestinationViewModel: filesystemAddDestination = new filesystemAddDestination(this.destinations);
        //    addDestinationViewModel
        //        .creationTask
        //        .done((destinationUrl: string) => this.addDestinationUrl(new synchronizationDestination(fs, destinationUrl)));
        //    app.showDialog(addDestinationViewModel);
        //});
    }

    private addDestinationUrl(url: synchronizationDestination) {
        //var fs = this.activeFilesystem();
        //if (fs) {
        //    var self = this;
        //    new saveDestinationCommand(fs, url).execute()
        //        .done(x => self.modelPolling());
        //}
    }

    synchronizeWithDestination(destination: string) {
        var fs = this.activeFilesystem();
        if (fs) {
            new synchronizeWithDestinationCommand(fs, destination).execute();
        }
    }

    deleteDestination(destination: synchronizationDestinationDto) {
        var fs = this.activeFilesystem();
        var self = this;
        if (fs) {
            new deleteDestinationCommand(fs, destination).execute()
                .done(x=> {
                    self.modelPolling();
                });
        }
    }

    modelPolling() {

        var fs = this.activeFilesystem();
        if (fs) {
            new getDestinationsCommand(fs).execute()
                .done(data => this.destinations(data));

            new getFilesConflictsCommand(fs).execute()
                .done(x => this.conflicts(x));
        }
    }

    loadConflictsAndDestinations() : JQueryPromise<any> {
        var fs = this.activeFilesystem();
        if (fs) {
            var deferred = $.Deferred();
            var destinationsTask = new getDestinationsCommand(fs).execute()
                .done(data => this.destinations(data));

            var conflictsTask = new getFilesConflictsCommand(fs).execute()
                .done(x => this.conflicts(x));

            var combined = $.when(destinationsTask, conflictsTask);

            combined.done(() => deferred.resolve());

            return deferred;
        }
    }

    collapseAll() {
        $(".synchronization-group-content").collapse('hide');
    }

    expandAll() {
        $(".synchronization-group-content").collapse('show');
    }

    resolveWithLocalVersion() {

        var message = this.selectedConflicts().length == 1 ?
            "Are you sure you want to resolve the conflict for file <b>" + this.selectedConflicts()[0] + "</b> by choosing the local version?" :
            "Are you sure you want to resolve the conflict for <b>" + this.selectedConflicts().length + "</b> selected files by choosing the local version?";

        require(["viewmodels/filesystem/resolveConflict"], resolveConflict => {
            var resolveConflictViewModel: resolveConflict = new resolveConflict(message, "Resolve conflict with local");
            resolveConflictViewModel
                .resolveTask
                .done(x => {
                    var fs = this.activeFilesystem();

                    for (var i = 0; i < this.selectedConflicts().length;  i++) {
                        var conflict = this.selectedConflicts()[i];
                        new resolveConflictCommand(conflict, 1, fs).execute()
                        .done(alert("Conflicts resolved!"));
                    }
                });
            app.showDialog(resolveConflictViewModel);
        });
    }

    resolveWithRemoteVersion() {

        var message = this.selectedConflicts().length == 1 ?
            "Are you sure you want to resolve the conflict for file <b>" + this.selectedConflicts()[0] + "</b> by choosing the remote version?" :
            "Are you sure you want to resolve the conflict for <b>" + this.selectedConflicts().length + "</b> selected files by choosing the remote version?";

        require(["viewmodels/filesystem/resolveConflict"], resolveConflict => {
            var resolveConflictViewModel: resolveConflict = new resolveConflict(message, "Resolve conflict with remote");
            resolveConflictViewModel
                .resolveTask
                .done(x => alert("Conflict resolved remotely"));
            app.showDialog(resolveConflictViewModel);
        });

    }

    fileSystemChanged(fs: filesystem) {
        if (fs) {
            this.modelPollingStop();
            this.loadConflictsAndDestinations().always(() => {
                this.modelPollingStart();
            });
        }
    }
}

export = synchronization;
