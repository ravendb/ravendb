import dialog = require("plugins/dialog");
import createResourceBase = require("viewmodels/resources/createResourceBase");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import resourcesManager = require("common/shell/resourcesManager");

class createCounterStorage extends createResourceBase {
    resourceNameCapitalString = "Counter storage";
    resourceNameString = "counter storage";

    constructor(parent: dialogViewModelBase) {
        super(resourcesManager.default.counterStorages, parent);
        this.storageEngine("voron");
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
