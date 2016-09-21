import resource = require("models/resources/resource");

import alertArgs = require("common/alertArgs");
import alertType = require("common/alertType");
import EVENTS = require("common/constants/events");

import recentErrors = require("viewmodels/common/recentErrors");

class notificationCenterRecentErrors {

    recordedErrors = ko.observableArray<alertArgs>(); //TODO: 
    currentAlert = ko.observable<alertArgs>(); //TODO:
    queuedAlert: alertArgs; //TODO:

    constructor() {
        ko.postbox.subscribe(EVENTS.NotificationCenter.Alert, (alert: alertArgs) => this.showAlert(alert));


        
        //TODO: this.globalChangesApi.watchDocsStartingWith("Raven/Alerts", () => this.fetchSystemDatabaseAlerts()) //TODO:
    }

    fetchSystemDatabaseAlerts() { //TODO:
        /* TODO
        new getDocumentWithMetadataCommand("Raven/Alerts", this.systemDatabase)
            .execute()
            .done((doc: documentClass) => {
                //
            });*/
    }

    showErrorsDialog() { //TODO:
        var errorDetails: recentErrors = new recentErrors(this.recordedErrors);
        //TODO: app.showDialog(errorDetails);
    }

    //TODO:
   

    //TODO: move to notification center code
    showAlert(alert: alertArgs) {
        if (alert.displayInRecentErrors && (alert.type === alertType.danger || alert.type === alertType.warning)) {
            this.recordedErrors.unshift(alert);
        }

        var currentAlert = this.currentAlert();
        if (currentAlert) {
            this.queuedAlert = alert;
            this.closeAlertAndShowNext(currentAlert);
        } else {
            this.currentAlert(alert);
            var fadeTime = 2000; // If there are no pending alerts, show it for 2 seconds before fading out.
            /*            if (alert.title.indexOf("Changes stream was disconnected.") == 0) {
                            fadeTime = 100000000;
                        }*/
            if (alert.type === alertType.danger || alert.type === alertType.warning) {
                fadeTime = 5000; // If there are pending alerts, show the error alert for 4 seconds before fading out.
            }
            setTimeout(() => {
                this.closeAlertAndShowNext(alert);
            }, fadeTime);
        }
    }

    //TODO: move to notifcation center
    closeAlertAndShowNext(alertToClose: alertArgs) {
        var alertElement = $('#' + alertToClose.id);
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

    //TODO: move to notification center
    onAlertHidden() {
        this.currentAlert(null);
        var nextAlert = this.queuedAlert;
        if (nextAlert) {
            this.queuedAlert = null;
            this.showAlert(nextAlert);
        }
    }

    

}

export = notificationCenterRecentErrors;