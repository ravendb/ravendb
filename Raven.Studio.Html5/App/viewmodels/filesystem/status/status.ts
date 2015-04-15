import app = require("durandal/app");
import appUrl = require("common/appUrl");
import pagedList = require("common/pagedList");
import shell = require("viewmodels/shell");
import viewModelBase = require("viewmodels/viewModelBase");
import synchronizationDetail = require("models/filesystem/synchronizationDetail");
import changeSubscription = require('common/changeSubscription');
import getSyncOutgoingActivitiesCommand = require("commands/filesystem/getSyncOutgoingActivitiesCommand");
import getSyncIncomingActivitiesCommand = require("commands/filesystem/getSyncIncomingActivitiesCommand");
import synchronizeNowCommand = require("commands/filesystem/synchronizeNowCommand");
import resetIndexConfirm = require("viewmodels/filesystem/status/resetIndexConfirm");

class status extends viewModelBase {

    pendingActivity = ko.observableArray<synchronizationDetail>();
    activeActivity = ko.observableArray<synchronizationDetail>();
    appUrls: computedAppUrls;

    outgoingActivity = ko.computed(() => {
        var pendingOutgoing = ko.utils.arrayFilter(this.pendingActivity(), (item) => { return item.Direction() === synchronizationDirection.Outgoing; });
        var activeOutgoing = ko.utils.arrayFilter(this.activeActivity(), (item) => { return item.Direction() === synchronizationDirection.Outgoing; });
        var allActivity = new Array<synchronizationDetail>();
        allActivity.pushAll(activeOutgoing);
        allActivity.pushAll(pendingOutgoing);
        return allActivity.slice(0, 50);
    });
    incomingActivity = ko.computed(() => {
        var pendingIncoming = ko.utils.arrayFilter(this.pendingActivity(), (item) => { return item.Direction() === synchronizationDirection.Incoming; });
        var activeIncoming = ko.utils.arrayFilter(this.activeActivity(), (item) => { return item.Direction() === synchronizationDirection.Incoming; });
        var allActivity = new Array <synchronizationDetail>();
        allActivity.pushAll(activeIncoming);
        allActivity.pushAll(pendingIncoming);
        return allActivity.slice(0, 50);
    });

    isOutgoingActivityVisible = ko.computed(() => true);

    incomingActivityPagedList = ko.observable<pagedList>();
    isIncomingActivityVisible = ko.computed(() => true);
    isFsSyncUpToDate: boolean = true;

    activate(args) {
        super.activate(args);

        this.appUrls = appUrl.forCurrentFilesystem();

        new getSyncOutgoingActivitiesCommand(this.activeFilesystem()).execute()
            .done((x: synchronizationDetail[]) => {
                for (var i = 0; i < x.length; i++) {
                    this.addOrUpdateActivity(x[i]);
                }
            });

        new getSyncIncomingActivitiesCommand(this.activeFilesystem()).execute()
            .done( (x : synchronizationDetail[]) => {
                for (var i = 0; i < x.length; i++) {
                    this.addOrUpdateActivity(x[i]);
                }
            });

        if (this.outgoingActivity().length == 0) {
            $("#outgoingActivityCollapse").collapse();
        }
    }

    createNotifications(): Array<changeSubscription> {
        return [ shell.currentResourceChangesApi().watchFsSync((e: synchronizationUpdateNotification) => this.processFsSync(e)) ];
    }

    private processFsSync(e: synchronizationUpdateNotification) {
        // treat notifications events
        this.isFsSyncUpToDate = false;

        var activity = new synchronizationDetail(e, this.getActionDescription(e.Action));
        
        if (e.Action != synchronizationAction.Finish) {
            this.addOrUpdateActivity(activity);
        }
        else {
            setTimeout(() => {
                this.activeActivity.remove(item => item.fileName() == e.FileName && item.Type() == e.Type);
                this.pendingActivity.remove(item => item.fileName() == e.FileName && item.Type() == e.Type);
            }, 3000);
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

    resetIndex() {
        var resetIndexVm = new resetIndexConfirm(this.activeFilesystem());
        app.showDialog(resetIndexVm);
    }

    private addOrUpdateActivity(e: synchronizationDetail) {
        var matchingActivity = this.getMatchingActivity(e);
        if (!matchingActivity) {
            if (e.Status() === "Active") {
                this.activeActivity.push(e);
            }
            else {
                this.pendingActivity.push(e);
            }
        }
        else if (matchingActivity.Status() === "Pending" && e.Status() === "Active") {
            this.pendingActivity.remove(matchingActivity);
            this.activeActivity.push(e);
        }
        else {
            matchingActivity.Status(e.Status());
        }
    }

    private getMatchingActivity(e: synchronizationDetail) : synchronizationDetail {
        var match = ko.utils.arrayFirst(this.pendingActivity(), (item) =>  {
            return item.fileName() === e.fileName() && item.Type() === e.Type();
        });

        if (!match) {
            match = ko.utils.arrayFirst(this.activeActivity(), (item) => {
                return item.fileName() === e.fileName() && item.Type() === e.Type();
            });
        }

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