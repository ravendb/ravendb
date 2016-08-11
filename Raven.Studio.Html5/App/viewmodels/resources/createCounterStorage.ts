import dialog = require("plugins/dialog");
import createResourceBase = require("viewmodels/resources/createResourceBase");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import shell = require("viewmodels/shell");

class createCounterStorage extends createResourceBase {
    resourceNameCapitalString = "Counter storage";
    resourceNameString = "counter storage";

    constructor(parent: dialogViewModelBase) {
        super(shell.counterStorages, parent);
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

export = createCounterStorage;
