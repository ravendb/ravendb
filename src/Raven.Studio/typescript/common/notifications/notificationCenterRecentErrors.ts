import alertArgs = require("common/alertArgs");
import alertType = require("common/alertType");
import EVENTS = require("common/constants/events");
import app = require("durandal/app");

import recentErrors = require("viewmodels/common/recentErrors");

class notificationCenterRecentErrors {

    recordedErrors = ko.observableArray<alertArgs>();
    currentAlert = ko.observable<alertArgs>();

    private queuedAlerts: Array<alertArgs> = [];

    constructor() {
        ko.postbox.subscribe(EVENTS.NotificationCenter.Alert, (alert: alertArgs) => this.showAlert(alert));
        //TODO: this.globalChangesApi.watchDocsStartingWith("Raven/Alerts", () => this.fetchSystemDatabaseAlerts())  - do we need watch for this document - it depends on RavenDB-5313
    }

    private fetchSystemDatabaseAlerts() { //TODO: maybe we should use name: fetchGlobalAlerts as we don't have notation of sys db right now
        /* TODO
        new getDocumentWithMetadataCommand("Raven/Alerts", this.systemDatabase)
            .execute()
            .done((doc: documentClass) => {
                //
            });*/
    }

    dismissRecentError(alert: alertArgs) {
        this.recordedErrors.remove(alert);
    }

    showErrorsDialog() {
        const errorDetails: recentErrors = new recentErrors(this.recordedErrors);
        app.showBootstrapDialog(errorDetails);
    }

    showAlert(alert: alertArgs) {
        if (alert.displayInRecentErrors && !this.recordedErrors.contains(alert)) {
            this.recordedErrors.unshift(alert);
        }

        const currentAlert = this.currentAlert();
        if (currentAlert) {
            this.queuedAlerts.unshift(alert);
            this.closeAlertAndShowNext(currentAlert);
        } else {
            this.currentAlert(alert);

            const fadeTime = alert.type === alertType.danger || alert.type === alertType.warning ? 5000 : 2000;

            setTimeout(() => this.closeAlertAndShowNext(alert), fadeTime);
        }
    }

    private closeAlertAndShowNext(alertToClose: alertArgs) {
        const alertElement = $('#' + alertToClose.id);
        if (alertElement.length === 0) {
            return;
        }

        // If the mouse is over the alert, keep it around.
        if (alertElement.is(":hover")) {
            setTimeout(() => this.closeAlertAndShowNext(alertToClose), 1000);
        } else {
            alertElement.alert("close");
        }
    }

    onAlertHidden() {
        this.currentAlert(null);
        const nextAlert = this.queuedAlerts.shift();
        if (nextAlert) {
            this.showAlert(nextAlert);
        }
    }

}

export = notificationCenterRecentErrors;