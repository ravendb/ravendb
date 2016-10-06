import resource = require("models/resources/resource");

import alertArgs = require("common/alertArgs");
import notificationCenterOperations = require("common/notifications/notificationCenterOperations");
import notificationCenterRecentErrors = require("common/notifications/notificationCenterRecentErrors");
import notificationCenterAlerts = require("common/notifications/notificationCenterAlerts");

class notificationCenter {
    static instance = new notificationCenter();

    showNotifications = ko.observable<boolean>(false);

    operations = new notificationCenterOperations();
    recentErrors = new notificationCenterRecentErrors();
    alerts = new notificationCenterAlerts();

    totalItemsCount: KnockoutComputed<number>;
    alertCountAnimation = ko.observable<boolean>();

    constructor() {
        this.initializeObservables();
    }

    private initializeObservables() {
        this.totalItemsCount = ko.pureComputed(() => {
            var ops = this.operations.watchedOperations().length;
            var errors = this.recentErrors.recordedErrors().length;
            var alerts = this.alerts.alerts().length;
            return ops + errors + alerts;
        });
        this.totalItemsCount.subscribe((count: number) => {
            if (count) {
                this.alertCountAnimation(false);
                setTimeout(() => this.alertCountAnimation(true));
            } else {
                this.alertCountAnimation(false);
            }
        });
    }

    monitorOperation<TProgress extends Raven.Client.Data.IOperationProgress,
        TResult extends Raven.Client.Data.IOperationResult>(rs: resource,
        operationId: number,
        onProgress: (progress: TProgress) => void = null): JQueryPromise<TResult> {
        return this.operations.monitorOperation(rs, operationId, onProgress);
    }

    killOperation(operationId: number) {
       this.operations.killOperation(operationId);
    }

    dismissOperation(operationId: number, saveOperations: boolean = true) {
        this.operations.dismissOperation(operationId, saveOperations);
    }

    dismissRecentError(alert: alertArgs) {
        this.recentErrors.dismissRecentError(alert);
    }

}

export = notificationCenter;