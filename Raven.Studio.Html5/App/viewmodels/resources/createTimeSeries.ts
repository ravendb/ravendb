import createResourceBase = require("viewmodels/resources/createResourceBase");
import dialog = require("plugins/dialog");
import timeSeries = require("models/timeSeries/timeSeries");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");

class createTimeSeries extends createResourceBase {
    creationTask = $.Deferred();
    creationTaskStarted = false;
   
    resourceNameCapitalString = "Time Series";
    resourceNameString = "time series storage";

    constructor(private timeSeries: KnockoutObservableArray<timeSeries>, licenseStatus: KnockoutObservable<licenseStatusDto>, private parent: dialogViewModelBase) {
        super(timeSeries, licenseStatus);
    }

    deactivate() {
        // If we were closed via X button or other dialog dismissal, reject the deletion task since
        // we never started it.
        if (!this.creationTaskStarted) {
            this.creationTask.reject();
        }
    }

    nextOrCreate() {
        // For now we're just creating the time series.
        this.creationTaskStarted = true;
        dialog.close(this.parent);
        this.creationTask.resolve(this.resourceName(), this.getActiveBundles(), this.resourcePath());
    }

    private getActiveBundles(): string[] {
        var activeBundles: string[] = [];
        return activeBundles;
    }
}

export = createTimeSeries;