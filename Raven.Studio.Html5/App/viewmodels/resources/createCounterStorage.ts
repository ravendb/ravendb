import createResourceBase = require("viewmodels/resources/createResourceBase");
import dialog = require("plugins/dialog");
import counterStorage = require("models/counter/counterStorage");

class createCounterStorage extends createResourceBase {
    creationTask = $.Deferred();
    creationTaskStarted = false;
   
    resourceNameCapitalString = "Counter storage";
    resourceNameString = "counter storage";

    constructor(private counterStorages: KnockoutObservableArray<counterStorage>, private licenseStatus: KnockoutObservable<licenseStatusDto>, private parent: dialogViewModelBase) {
        super(counterStorages, licenseStatus);
    }

    deactivate() {
        // If we were closed via X button or other dialog dismissal, reject the deletion task since
        // we never started it.
        if (!this.creationTaskStarted) {
            this.creationTask.reject();
        }
    }

    nextOrCreate() {
        // For now we're just creating the filesystem.
        this.creationTaskStarted = true;
        dialog.close(this.parent);
        this.creationTask.resolve(this.resourceName(), this.getActiveBundles(), this.resourcePath());
    }

    private getActiveBundles(): string[] {
        var activeBundles: string[] = [];
        return activeBundles;
    }
}

export = createCounterStorage;