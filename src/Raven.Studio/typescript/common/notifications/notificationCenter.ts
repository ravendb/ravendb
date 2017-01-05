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
    noNewNotifications: KnockoutComputed<boolean>;

    private hideHandler = (e: Event) => {
        if (this.shouldConsumeHideEvent(e)) {
            this.showNotifications(false);
        }
    }

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
        this.noNewNotifications = ko.pureComputed(() => {
            return this.totalItemsCount() === 0;
        });

        this.showNotifications.subscribe((show: boolean) => {
            if (show) {
                window.addEventListener("click", this.hideHandler, true);
            } else {
                window.removeEventListener("click", this.hideHandler, true);
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

    showRecentErrorDialog(alert: alertArgs) {
        this.recentErrors.showRecentErrorDialog(alert);
    }

    private shouldConsumeHideEvent(e: Event) {
        return $(e.target).closest(".notification-center-container").length === 0
            && $(e.target).closest("#notification-toggle").length === 0;
    }
}

export = notificationCenter;