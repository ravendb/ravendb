import app = require("durandal/app");
import system = require("durandal/system");
import router = require("plugins/router");
import appUrl = require("common/appUrl");
import pagedList = require("common/pagedList");
import pagedResultSet = require("common/pagedResultSet");
import virtualTable = require("widgets/virtualTable/viewModel");

//models
import filesystem = require("models/filesystem/filesystem");
import synchronizationDetail = require("models/filesystem/synchronizationDetail");

//viewmodels
import viewModelBase = require("viewmodels/viewModelBase");

//commands
import synchronizeNowCommand = require("commands/filesystem/synchronizeNowCommand");
import getSyncOutgoingActivitiesCommand = require("commands/filesystem/getSyncOutgoingActivitiesCommand");
import getSyncIncomingActivitiesCommand = require("commands/filesystem/getSyncIncomingActivitiesCommand");

class synchronizationActivity extends viewModelBase {

    outgoingActivity = ko.observableArray<synchronizationDetail>();
    isOutgoingActivityVisible = ko.computed(() => true);

    incomingActivityPagedList = ko.observable<pagedList>();
    isIncomingActivityVisible = ko.computed(() => true);

    private activeFilesystemSubscription: any;

    static outgoingGridSelector = "#outgoingGrid";
    static incomingGridSelector = "#incomingGrid";

    canActivate(args: any) {
        this.loadSynchronizationActivity();
        return true;
    }

    activate(args) {
        super.activate(args);
        this.activeFilesystemSubscription = this.activeFilesystem.subscribe((fs: filesystem) => this.fileSystemChanged(fs));

        if (this.outgoingActivity().length == 0) {
            $("#outgoingActivityCollapse").collapse();
        }
    }

    deactivate() {
        super.deactivate();
        this.activeFilesystemSubscription.dispose();
    }

    modelPolling() {

        //var fs = this.activeFilesystem();
        //if (fs) {
        //    var incomingGrid = this.getIncomingActivityTable();
        //    if (incomingGrid) {
        //        incomingGrid.loadRowData();
        //    }

        //    new getSyncOutgoingActivitiesCommand(this.activeFilesystem()).execute()
        //        .done(x => {
        //        this.outgoingActivity(x);
        //            if (this.outgoingActivity().length > 0) {
        //                $("#outgoingActivityCollapse").collapse('show');
        //            }
        //            else {
        //                $("#outgoingActivityCollapse").collapse();
        //            }
        //        });
        //}
    }

    synchronizeNow() {
        var fs = this.activeFilesystem();
        if (fs) {
            new synchronizeNowCommand(fs).execute();
        }
    }

    loadSynchronizationActivity() {
        this.incomingActivityPagedList(this.createIncomingActivityPagedList());
    }

    createIncomingActivityPagedList(): pagedList {
        var fetcher = (skip: number, take: number) => this.incomingActivityFetchTask(skip, take);
        var list = new pagedList(fetcher);
        return list;
    }

    fileSystemChanged(fs: filesystem) {
        if (fs) {

            this.modelPollingStop();
            this.loadSynchronizationActivity();
            this.modelPollingStart();

        }
    }

    getIncomingActivityTable(): virtualTable {
        var gridContents = $(synchronizationActivity.incomingGridSelector).children()[0];
        if (gridContents) {
            return ko.dataFor(gridContents);
        }

        return null;
    }

    incomingActivityFetchTask(skip: number, take: number): JQueryPromise<pagedResultSet> {
        var task = new getSyncIncomingActivitiesCommand(this.activeFilesystem(), skip, take).execute();
        return task;
    }
}

export = synchronizationActivity;