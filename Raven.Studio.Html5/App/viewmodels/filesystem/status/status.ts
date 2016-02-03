import appUrl = require("common/appUrl");
import changesContext = require("common/changesContext");
import viewModelBase = require("viewmodels/viewModelBase");
import changeSubscription = require('common/changeSubscription');
import synchronizeNowCommand = require("commands/filesystem/synchronizeNowCommand");
import activityItems = require("viewmodels/filesystem/status/activityItems");

class status extends viewModelBase {

    pendingOutgoing: activityItems;
    activeOutgoing: activityItems;

    finishedIncoming: activityItems;
    activeIncoming: activityItems;

    appUrls: computedAppUrls;

    isFsSyncUpToDate: boolean = true;

    activate(args) {
        super.activate(args);

        this.appUrls = appUrl.forCurrentFilesystem();

        this.pendingOutgoing = new activityItems(this.activeFilesystem(), synchronizationActivity.Pending, synchronizationDirection.Outgoing);
        this.activeOutgoing = new activityItems(this.activeFilesystem(), synchronizationActivity.Active, synchronizationDirection.Outgoing);

        this.finishedIncoming = new activityItems(this.activeFilesystem(), synchronizationActivity.Finished, synchronizationDirection.Incoming);
        this.activeIncoming = new activityItems(this.activeFilesystem(), synchronizationActivity.Active, synchronizationDirection.Incoming);
    }

    createNotifications(): Array<changeSubscription> {
        return [changesContext.currentResourceChangesApi().watchFsSync((e: synchronizationUpdateNotification) => this.processFsSync(e)) ];
    }

    private processFsSync(e: synchronizationUpdateNotification) {
        // treat notifications events
        this.isFsSyncUpToDate = false;

        switch (e.Direction) {
            case "Outgoing":
                
                if (e.Action === "Enqueue") {
                    this.pendingOutgoing.refresh();
                }
                else if (e.Action === "Start" || e.Action === "Finish") {
                    this.activeOutgoing.refresh();
                    this.pendingOutgoing.refresh();
                }
                break;
            case "Incoming":

                if (e.Action === "Start") {
                    this.activeIncoming.refresh();
                }
                else if (e.Action === "Finish") {
                    this.activeIncoming.refresh();
                    this.finishedIncoming.refresh();
                }
                break;
            default:
                break;
        }
    }

    synchronizeNow() {
        var fs = this.activeFilesystem();
        if (fs) {
            new synchronizeNowCommand(fs).execute();
        }
    }

    private getActionDescription(action: synchronizationAction) {
        switch (action) {
            case synchronizationAction.Enqueue:
                return synchronizationActivity.Pending;
            case synchronizationAction.Start:
                return synchronizationActivity.Active;
            case synchronizationAction.Finish:
                return synchronizationActivity.Finished;
            default:
                return synchronizationActivity.Unknown;
        }
    }
}

export = status;
