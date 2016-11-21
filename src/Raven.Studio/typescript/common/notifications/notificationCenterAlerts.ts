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

import getDatabaseAlertsCommand = require("commands/alerts/getDatabaseAlertsCommand");
import getGlobalAlertsCommand = require("commands/alerts/getGlobalAlertsCommand");
import adminWatchClient = require("common/adminWatchClient");

class notificationCenterAlerts {

    alerts = ko.observableArray<alert>();

    activeResourceChangesApi: KnockoutObservable<changesApi>;

    constructor() {
        this.activeResourceChangesApi = changesContext.default.currentResourceChangesApi;

        this.onNewGlobalAlert();
        let globalAlertsSubscription = changesContext.default.globalChangesApi.subscribe(api => {
            api.watchAlerts(globalAlertEvent => {
                this.onNewGlobalAlert(globalAlertEvent);
            });

            globalAlertsSubscription.dispose();
        });

        ko.postbox.subscribe("ChangesApiReconnected",
            () => this.onReconnectWatchAndShowActiveResourceAlerts());
        $(window).bind("storage", (event) => this.onStorageEvent(event));
    }

    private onStorageEvent(event: JQueryEventObject) {
        const storageEvent = event.originalEvent as StorageEvent;

        const rs = this.activeResourceChangesApi().getResource();

        //TODO: match key

        //TODO: handle action
    }

    private onReconnectWatchAndShowActiveResourceAlerts() {
        const changes = this.activeResourceChangesApi();
        const rs = changes.getResource();

        changes.watchAlerts(newAlertEvent => {
            this.retrieveDatabaseAlerts(rs as database);
        });

    }

    private onNewGlobalAlert(msg?: Raven.Server.Alerts.GlobalAlertNotification) {
        new getGlobalAlertsCommand()
            .execute()
            .done((alerts: alert[]) => this.consolidateAlerts(alerts))
            .fail((response: JQueryXHR) =>
                messagePublisher.reportError("Failed to get alerts", response.responseText, response.statusText));
    }

    private retrieveDatabaseAlerts(resource: database) {
        new getDatabaseAlertsCommand(resource)
            .execute()
            .done((alerts: alert[]) => this.consolidateAlerts(alerts))
            .fail((response: JQueryXHR) =>
                messagePublisher.reportError("Failed to get alerts", response.responseText, response.statusText));
    }

    private consolidateAlerts(alerts: alert[]) {
        const currentAlertKeys = new Set(this.alerts().map(x => x.key));
        const newAlerts = alerts.filter(a => !currentAlertKeys.has(a.key));
        this.alerts.push(...newAlerts);
    }

}

export = notificationCenterAlerts;