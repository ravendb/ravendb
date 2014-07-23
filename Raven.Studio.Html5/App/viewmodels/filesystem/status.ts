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

    appUrls: computedAppUrls;

    activity = ko.observableArray<synchronizationDetail>();
    outgoingActivity = ko.computed(() => {
        return ko.utils.arrayFilter(this.activity(), (item) => { return item.Direction === synchronizationDirection.Outgoing; });
    });
    incomingActivity = ko.computed(() => {
        return ko.utils.arrayFilter(this.activity(), (item) => { return item.Direction === synchronizationDirection.Incoming; });
    });

    isOutgoingActivityVisible = ko.computed(() => true);

    incomingActivityPagedList = ko.observable<pagedList>();
    isIncomingActivityVisible = ko.computed(() => true);
    isFsSyncUpToDate: boolean = true;

    activate(args) {
        super.activate(args);

        this.appUrls = appUrl.forCurrentFilesystem();

        new getSyncOutgoingActivitiesCommand(this.activeFilesystem()).execute()
            .done(x => this.activity(x));

        new getSyncIncomingActivitiesCommand(this.activeFilesystem()).execute()
            .done(x => this.activity(x));

        if (this.outgoingActivity().length == 0) {
            $("#outgoingActivityCollapse").collapse();
        }
    }

    createNotifications(): Array<changeSubscription> {
        return [shell.currentResourceChangesApi().watchFsSync((e: synchronizationUpdateNotification) => this.processFsSync(e))];
    }

    private processFsSync(e: synchronizationUpdateNotification) {
        // treat notifications events
        this.isFsSyncUpToDate = false;

        if (e.Action != synchronizationAction.Finish) {
            this.addOrUpdateActivity(e);
        }
        else {
            setTimeout(() => this.activity.remove(item => { return item.fileName === e.FileName; }), 3000);
        }
    }

    synchronizeNow() {
        var fs = this.activeFilesystem();
        if (fs) {
            new synchronizeNowCommand(fs).execute();
        }
    }

    collapseAll() {
        $(".synchronization-group-content").collapse('hide');
    }

    expandAll() {
        $(".synchronization-group-content").collapse('show');
    }

    private addOrUpdateActivity(e: synchronizationUpdateNotification) {

        if (!this.activityContains(e)) {
            this.activity.push(new synchronizationDetail({
                FileName: e.FileName,
                DestinationUrl: e.DestinationFileSystemUrl,
                FileETag: "",
                Type: e.Type,
                Direction: e.SynchronizationDirection
            }, this.getActionDescription(e.Action)));
        }
        else {
            console.log(e.FileName + " has been modified");
            this.activity.remove((item) => { return item.fileName === e.FileName; });
            this.activity.push(new synchronizationDetail({
                FileName: e.FileName,
                DestinationUrl: e.DestinationFileSystemUrl,
                FileETag: "",
                Type: e.Type,
                Direction: e.SynchronizationDirection
            }, this.getActionDescription(e.Action)));
        }
    }

    private activityContains(e: synchronizationUpdateNotification) {
        var match = ko.utils.arrayFirst(this.activity(), (item) =>  {
            return item.fileName === e.FileName;
        });

        return match;
    }

    private getActionDescription(action: synchronizationAction) {
        switch (action) {
            case synchronizationAction.Enqueue:
                return "Pending";
            case synchronizationAction.Start:
                return "Active";
            case synchronizationAction.Finish:
                return "Finished";
            default:
                return "Unknown";
        }
    }
}

export = status;