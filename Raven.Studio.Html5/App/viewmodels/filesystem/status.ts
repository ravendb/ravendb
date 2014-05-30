import app = require("durandal/app");
import system = require("durandal/system");
import router = require("plugins/router");
import appUrl = require("common/appUrl");
import pagedList = require("common/pagedList");
import pagedResultSet = require("common/pagedResultSet");
import virtualTable = require("widgets/virtualTable/viewModel");
import shell = require("viewmodels/shell");

import filesystem = require("models/filesystem/filesystem");
import synchronizationDetail = require("models/filesystem/synchronizationDetail");

import viewModelBase = require("viewmodels/viewModelBase");

import getSyncOutgoingActivitiesCommand = require("commands/filesystem/getSyncOutgoingActivitiesCommand");
import getSyncIncomingActivitiesCommand = require("commands/filesystem/getSyncIncomingActivitiesCommand");
import synchronizeNowCommand = require("commands/filesystem/synchronizeNowCommand");

import changeSubscription = require('models/changeSubscription');
import changesApi = require("common/changesApi");

class status extends viewModelBase {

    outgoingActivity = ko.observableArray<synchronizationDetail>();
    isOutgoingActivityVisible = ko.computed(() => true);

    incomingActivityPagedList = ko.observable<pagedList>();
    isIncomingActivityVisible = ko.computed(() => true);
    isFsSyncUpToDate: boolean = true;

    activitiesSubscription: changeSubscription;

    static outgoingGridSelector = "#outgoingGrid";
    static incomingGridSelector = "#incomingGrid";

    private activeFilesystemSubscription: any;

    canActivate(args: any) {
        this.loadSynchronizationActivity();

        return true;
    }

    activate(args) {
        super.activate(args);
        this.activeFilesystemSubscription = this.activeFilesystem.subscribe((fs: filesystem) => this.fileSystemChanged(fs));

        // treat notifications events
        this.activitiesSubscription = shell.currentFsChangesApi().watchFsSync((e: synchronizationUpdateNotification) => {
            this.isFsSyncUpToDate = false;

            switch (e.SynchronizationDirection) {
                case synchronizationDirection.Outgoing:
                    switch (e.Action) {
                        case synchronizationAction.Start:
                            if (!this.outgoingContains(e)) {
                                this.outgoingActivity.push(new synchronizationDetail({
                                    FileName: e.FileName,
                                    DestinationUrl: e.DestinationFileSystemUrl,
                                    FileETag: "",
                                    Type: e.Type
                                }));
                            }
                            else {
                                console.log(e.FileName + " has been modified");
                            }
                            break;
                        case synchronizationAction.Finish:
                            console.log(e.FileName + " sync has finished");
                            setTimeout(() => this.outgoingActivity.remove(item => { return item.fileName === e.FileName; }), 1000);
                            break;
                        default:
                            console.error("unknown notification action");
                    }
                    break;
                case synchronizationDirection.Incoming:
                    console.log("incomming notification");
                    break;
                default:
                    console.error("unknown notification direction");
            }
        });

        if (this.outgoingActivity().length == 0) {
            $("#outgoingActivityCollapse").collapse();
        }
    }

    deactivate() {
        super.deactivate();
        this.activeFilesystemSubscription.dispose();
    }

    modelPolling() {

        var fs = this.activeFilesystem();
        if (fs) {

            //var incomingGrid = this.getIncomingActivityTable();
            //if (incomingGrid) {
            //    incomingGrid.loadRowData();
            //}

            //new getSyncOutgoingActivitiesCommand(this.activeFilesystem()).execute()
            //    .done(x => {
            //        this.outgoingActivity(x);
            //        if (this.outgoingActivity().length > 0) {
            //            $("#outgoingActivityCollapse").collapse('show');
            //        }
            //        else {
            //            $("#outgoingActivityCollapse").collapse();
            //        }
            //    });
        }
    }

    loadSynchronizationActivity() {
        this.incomingActivityPagedList(this.createIncomingActivityPagedList());
    }

    synchronizeNow() {
        var fs = this.activeFilesystem();
        if (fs) {
            new synchronizeNowCommand(fs).execute();
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

    collapseAll() {
        $(".synchronization-group-content").collapse('hide');
    }

    expandAll() {
        $(".synchronization-group-content").collapse('show');
    }

    getIncomingActivityTable(): virtualTable {
        var gridContents = $(status.incomingGridSelector).children()[0];
        if (gridContents) {
            return ko.dataFor(gridContents);
        }

        return null;
    }

    fileSystemChanged(fs: filesystem) {
        if (fs) {
            this.modelPollingStop();
            this.loadSynchronizationActivity();
            this.modelPollingStart();
        }
    }

    private outgoingContains(e: synchronizationUpdateNotification) {
        var match = ko.utils.arrayFirst(this.outgoingActivity(), (item) =>  {
            return item.fileName === e.FileName;
        });

        return match;
    }

}

export = status;