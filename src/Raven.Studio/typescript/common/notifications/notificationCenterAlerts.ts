import resource = require("models/resources/resource");
import database = require("models/resources/database");
import alert = require("models/database/debug/alert");

import EVENTS = require("common/constants/events");

import watchedOperation = require("common/notifications/watchedOperation");
import messagePublisher = require("common/messagePublisher");
import changesApi = require("common/changesApi");
import changesContext = require("common/changesContext");

import killOperationCommand = require("commands/operations/killOperationCommand");


class notificationCenterAlerts {

    alerts = ko.observableArray<alert>();

    activeResourceChangesApi: KnockoutObservable<changesApi>;

    constructor() {
        this.activeResourceChangesApi = changesContext.default.resourceChangesApi;

        /* TODO
        this.onNewGlobalAlert();
        let globalAlertsSubscription = changesContext.default.globalChangesApi.subscribe(api => {
            api.watchAlerts(globalAlertEvent => {
                this.onNewGlobalAlert(globalAlertEvent);
            });

            globalAlertsSubscription.dispose();
        });

        ko.postbox.subscribe(EVENTS.ChangesApi.Reconnected,
            () => this.onReconnectWatchAndShowActiveResourceAlerts());
        $(window).bind("storage", (event) => this.onStorageEvent(event)); */
    }

    private onStorageEvent(event: JQueryEventObject) {
        const storageEvent = event.originalEvent as StorageEvent;

        const rs = this.activeResourceChangesApi().getResource();

        //TODO: match key

        //TODO: handle action
    }

    /* TODO
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
        const currentAlerts = this.alerts();
        const currentAlertKeys = new Set(currentAlerts.map(x => x.key));

        const alertsToUpdateDict = alerts.filter(a => currentAlertKeys.has(a.key))
            .reduce((result: { [key: string] : alert }, item: alert) => {
                result[item.key] = item;
                return result;
            }, {} as { [key: string] : alert });

        currentAlerts.forEach(alert => {
            const update = alertsToUpdateDict[alert.key];
            if (update) {
                alert.severity(update.severity());
                alert.message(update.message())
            }
        })
        
        const newAlerts = alerts.filter(a => !currentAlertKeys.has(a.key));
        this.alerts.push(...newAlerts);
    }*/

}

export = notificationCenterAlerts;