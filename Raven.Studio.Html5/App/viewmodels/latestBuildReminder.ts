import app = require("durandal/app");
import dialog = require("plugins/dialog");
import viewModelBase = require("viewmodels/viewModelBase");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");

class latestBuildReminder extends dialogViewModelBase {

    public dialogTask = $.Deferred();
    mute = ko.observable<boolean>(false);

    constructor(private latestServerBuildResult: latestServerBuildVersionDto, elementToFocusOnDismissal?: string) {
        super(elementToFocusOnDismissal);

        this.mute.subscribe(() => {
            if (this.mute()) {
                localStorage.setObject("LastServerBuildCheck", new Date());
            } else {
                localStorage.removeItem("LastServerBuildCheck");
            }
        });
    }

    detached() {
        super.detached();
        this.dialogTask.resolve();
    }

    close() {
        dialog.close(this);
    }
}

export = latestBuildReminder;