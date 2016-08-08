import dialog = require("plugins/dialog");
import createResourceBase = require("viewmodels/resources/createResourceBase");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import shell = require("viewmodels/shell");

class createTimeSeries extends createResourceBase {
    resourceNameCapitalString = "Time Series";
    resourceNameString = "time series";

    constructor(parent: dialogViewModelBase) {
        super(shell.timeSeries, parent);
        this.storageEngine("voron");
    }

    protected shouldReportUsage(): boolean {
        return false;
    }

    nextOrCreate() {
        this.creationTaskStarted = true;
        dialog.close(this.parent);
        this.creationTask.resolve(this.resourceName(), this.getActiveBundles(), this.resourcePath(), this.resourceTempPath());
        this.clearResourceName();
    }

    private getActiveBundles(): string[] {
        var activeBundles: string[] = [];
        return activeBundles;
    }
}

export = createTimeSeries;
