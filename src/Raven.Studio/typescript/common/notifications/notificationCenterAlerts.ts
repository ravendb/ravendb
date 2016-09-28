import resource = require("models/resources/resource");
import database = require("models/resources/database");
import alert = require("models/database/debug/alert");

import watchedOperation = require("common/notifications/watchedOperation");
import notificationCenterPersistanceStorage = require("common/notifications/notificationCenterPersistanceStorage");
import messagePublisher = require("common/messagePublisher");
import changesApi = require("common/changesApi");
import changesContext = require("common/changesContext");

import getOperationCommand = require("commands/operations/getOperationCommand");
import dismissOperationCommand = require("commands/operations/dismissOperationCommand");
import getRunningTasksCommand = require("commands/operations/getRunningTasksCommand");
import killOperationCommand = require("commands/operations/killOperationCommand");

import getOperationAlertsCommand = require("commands/operations/getOperationAlertsCommand");

class notificationCenterAlerts {

    alerts = ko.observableArray<alert>();

    activeChangesApi: KnockoutObservable<changesApi>;

    constructor() {
        this.activeChangesApi = changesContext.currentResourceChangesApi;
        ko.postbox.subscribe("ChangesApiReconnected", () => this.onReconnect());
        $(window).bind("storage", (event) => this.onStorageEvent(event));
    }
    

    private onStorageEvent(event: JQueryEventObject) {
        const storageEvent = event.originalEvent as StorageEvent;

        const rs = this.activeChangesApi().getResource();

        //TODO: match key

        //TODO: handle action
    }

    private onReconnect() {
        const changes = this.activeChangesApi();
        const rs = changes.getResource();

        changes.watchAlerts(this.onStatus);

        new getOperationAlertsCommand(rs as database)
            .execute()
            .done((alerts: alert[]) => {
                this.alerts(alerts); //TODO: filter alerts
            })
            .fail((response: JQueryXHR) => messagePublisher.reportError("Failed to get alerts", response.responseText, response.statusText));
    }

    private onStatus(status: Raven.Server.Web.Operations.AlertNotification) {
        
        //TODO:
    }

}

export = notificationCenterAlerts;