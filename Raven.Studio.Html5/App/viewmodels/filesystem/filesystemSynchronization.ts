import app = require("durandal/app");
import system = require("durandal/system");
import router = require("plugins/router");
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
import getSyncOutgoingActivitiesCommand = require("commands/filesystem/getSyncOutgoingActivitiesCommand");
import getSyncIncomingActivitiesCommand = require("commands/filesystem/getSyncIncomingActivitiesCommand");
import saveDestinationCommand = require("commands/filesystem/saveDestinationCommand");
import deleteDestinationCommand = require("commands/filesystem/deleteDestinationCommand");
import synchronizeNowCommand = require("commands/filesystem/synchronizeNowCommand");
import synchronizeWithDestinationCommand = require("commands/filesystem/synchronizeWithDestinationCommand");
import resolveConflictCommand = require("commands/filesystem/resolveConflictCommand");
import virtualTable = require("widgets/virtualTable/viewModel");

import filesystemAddDestination = require("viewmodels/filesystem/filesystemAddDestination");
import resolveConflict = require("viewmodels/filesystem/resolveConflict");
import filesystem = require("models/filesystem/filesystem");

class filesystemSynchronization extends viewModelBase {

    destinations = ko.observableArray<string>();
    isDestinationsVisible = ko.computed(() => this.destinations().length > 0);

    conflicts = ko.observableArray<string>();
    selectedConflicts = ko.observableArray<string>();
    isConflictsVisible = ko.computed(() => this.conflicts().length > 0);

    outgoingActivityPagedList = ko.observable<pagedList>();
    //isOutgoingActivityVisible = ko.computed(() => this.outgoingActivityPagedList() ? this.outgoingActivityPagedList().totalResultCount() > 0 : false);
    isOutgoingActivityVisible = ko.computed(() => true);
    
    incomingActivityPagedList = ko.observable<pagedList>();   
    //isIncomingActivityVisible = ko.computed(() => this.incomingActivityPagedList() ? this.incomingActivityPagedList().totalResultCount() > 0 : false);
    isIncomingActivityVisible = ko.computed(() => true);
          
    private router = router;
    private pollingInterval: any;
    synchronizationUrl = appUrl.forCurrentDatabase().filesystemSynchronization;

    static outgoingGridSelector = "#outgoingGrid";
    static incomingGridSelector = "#incomingGrid";

    constructor() {
        super();
    }

    canActivate(args: any) {
        this.loadActivity();

        return true;
    }

    activate(args) {
        super.activate(args);
        this.activeFilesystem.subscribe((fs: filesystem) => this.fileSystemChanged(fs));

        this.startPolling();
    }

    startPolling() {
        this.pollingInterval = setInterval(() => { this.modelPolling() }, 10000);
    }

    deactivate() {
        window.clearInterval(this.pollingInterval);
    }

    addDestination() {
        var fs = this.activeFilesystem();
        require(["viewmodels/filesystem/filesystemAddDestination"], filesystemAddDestination => {
            var addDestinationViewModel: filesystemAddDestination = new filesystemAddDestination(this.destinations);
            addDestinationViewModel
                .creationTask
                .done((destinationUrl: string) => this.addDestinationUrl(new synchronizationDestination(fs, destinationUrl)));
            app.showDialog(addDestinationViewModel);
        });
    }

    private addDestinationUrl(url: synchronizationDestination) {
        var fs = this.activeFilesystem();
        if (fs) {
            var self = this;
            new saveDestinationCommand(fs, url).execute()
                .done(x => self.forceModelPolling());
        }
    }

    loadActivity() {
        var incomingGrid = this.getIncomingActivityTable();
        this.outgoingActivityPagedList(this.createOutgoingActivityPagedList());
        this.incomingActivityPagedList(this.createIncomingActivityPagedList());
    }

    synchronizeNow() {
        var fs = this.activeFilesystem();
        if (fs) {
            new synchronizeNowCommand(fs).execute();
        }
    }

    synchronizeWithDestination(destination: string) {
        var fs = this.activeFilesystem();
        if (fs) {
            new synchronizeWithDestinationCommand(fs, destination).execute();
        }
    }

    deleteDestination(destination: string) {
        var fs = this.activeFilesystem();
        var self = this;
        if (fs) {
            new deleteDestinationCommand(fs, destination).execute()
                .done(x => self.forceModelPolling());
        }
    }

    modelPolling() {

        var fs = this.activeFilesystem();
        if (fs) {
            new getDestinationsCommand(fs).execute()
                .done(data => this.destinations(data));

            new getFilesConflictsCommand(fs).execute()
                .done(x => this.conflicts(x));

            var incomingGrid = this.getIncomingActivityTable();
            if (incomingGrid) {
                incomingGrid.loadRowData();
            }

            var outgoingGrid = this.getOutgoingActivityTable();
            if (outgoingGrid) {
                outgoingGrid.loadRowData();
            }
        }
    }

    createIncomingActivityPagedList(): pagedList {
        var fetcher = (skip: number, take: number) => this.incomingActivityFetchTask(skip, take);
        var list = new pagedList(fetcher);
        return list;
    }

    incomingActivityFetchTask(skip: number, take: number): JQueryPromise<pagedResultSet> {
        var task = new getSyncIncomingActivitiesCommand(this.activeFilesystem(), skip, take).execute();
        return task;
    }

    createOutgoingActivityPagedList(): pagedList {
        var fetcher = (skip: number, take: number) => this.outgoingActivityFetchTask(skip, take);
        var list = new pagedList(fetcher);
        return list;
    }

    outgoingActivityFetchTask(skip: number, take: number): JQueryPromise<pagedResultSet> {
        var task = new getSyncOutgoingActivitiesCommand(this.activeFilesystem(), skip, take).execute();
        return task;
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

    getOutgoingActivityTable(): virtualTable {
        var gridContents = $(filesystemSynchronization.outgoingGridSelector).children()[0];
        if (gridContents) {
            return ko.dataFor(gridContents);
        }

        return null;
    }

    getIncomingActivityTable(): virtualTable {
        var gridContents = $(filesystemSynchronization.incomingGridSelector).children()[0];
        if (gridContents) {
            return ko.dataFor(gridContents);
        }

        return null;
    }

    fileSystemChanged(fs: filesystem) {
        if (fs) {
            window.clearInterval(this.pollingInterval);
            this.loadActivity();
            this.startPolling();
        }
    }
}

export = filesystemSynchronization;