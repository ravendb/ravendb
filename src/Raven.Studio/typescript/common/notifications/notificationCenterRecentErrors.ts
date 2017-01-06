import alertArgs = require("common/alertArgs");
import EVENTS = require("common/constants/events");
import app = require("durandal/app");

import recentErrors = require("viewmodels/common/recentErrors");

class notificationCenterRecentErrors {

    recordedErrors = ko.observableArray<alertArgs>();

    constructor() {
        ko.postbox.subscribe(EVENTS.NotificationCenter.RecentError, (alert: alertArgs) => this.showAlert(alert));
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

    showRecentErrorDialog(alert: alertArgs) {
        //TODO: using old dialog to display sigle item, it will be changed in future
        const errorDetails: recentErrors = new recentErrors(ko.observableArray<alertArgs>([alert]));
        app.showBootstrapDialog(errorDetails);
    }

    showAlert(alert: alertArgs) {
        if (!_.includes(this.recordedErrors(), alert)) {
            this.recordedErrors.unshift(alert);
        }
    }
}

export = notificationCenterRecentErrors;