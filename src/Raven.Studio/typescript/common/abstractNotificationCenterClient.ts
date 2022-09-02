/// <reference path="../../typings/tsd.d.ts" />

import database = require("models/resources/database");
import changeSubscription = require("common/changeSubscription");
import changesCallback = require("common/changesCallback");
import eventsWebSocketClient = require("common/eventsWebSocketClient");

abstract class abstractNotificationCenterClient extends eventsWebSocketClient<Raven.Server.NotificationCenter.Notifications.Notification> {

    constructor(db: database) {
        super(db);
    }

    protected allReconnectHandlers = ko.observableArray<changesCallback<void>>();
    protected allAlertsHandlers = ko.observableArray<changesCallback<Raven.Server.NotificationCenter.Notifications.AlertRaised>>();
    protected allOperationsHandlers = ko.observableArray<changesCallback<Raven.Server.NotificationCenter.Notifications.OperationChanged>>();
    protected watchedOperationsChanged = new Map<number, KnockoutObservableArray<changesCallback<Raven.Server.NotificationCenter.Notifications.OperationChanged>>>();
    protected allNotificationUpdatedHandlers = ko.observableArray<changesCallback<Raven.Server.NotificationCenter.Notifications.NotificationUpdated>>();
    protected allPerformanceHintsHandlers = ko.observableArray<changesCallback<Raven.Server.NotificationCenter.Notifications.PerformanceHint>>();

    protected onOpen() {
        super.onOpen();
        this.connectToWebSocketTask.resolve();

        this.fireEvents<void>(this.allReconnectHandlers(), undefined, () => true);
    }

    protected onMessage(actionDto: Raven.Server.NotificationCenter.Notifications.Notification) {
        const actionType = actionDto.Type;

        switch (actionType) {
            case "PerformanceHint": {
                const performanceHintDto = actionDto as Raven.Server.NotificationCenter.Notifications.PerformanceHint;
                this.fireEvents<Raven.Server.NotificationCenter.Notifications.PerformanceHint>(this.allPerformanceHintsHandlers(), performanceHintDto, () => true);
                break;
            }

            case "AlertRaised": {
                const alertDto = actionDto as Raven.Server.NotificationCenter.Notifications.AlertRaised;
                this.fireEvents<Raven.Server.NotificationCenter.Notifications.AlertRaised>(this.allAlertsHandlers(), alertDto, () => true);
                break;
            }

            case "OperationChanged": {
                const operationDto = actionDto as Raven.Server.NotificationCenter.Notifications.OperationChanged;
                this.fireEvents<Raven.Server.NotificationCenter.Notifications.OperationChanged>(this.allOperationsHandlers(), operationDto, () => true);

                this.watchedOperationsChanged.forEach((callbacks, key) => {
                    this.fireEvents<Raven.Server.NotificationCenter.Notifications.OperationChanged>(callbacks(), operationDto, (event) => event.OperationId === key);
                });

                break;
            }
            case "NotificationUpdated": {
                const notificationUpdatedDto = actionDto as Raven.Server.NotificationCenter.Notifications.NotificationUpdated;
                this.fireEvents<Raven.Server.NotificationCenter.Notifications.NotificationUpdated>(this.allNotificationUpdatedHandlers(), notificationUpdatedDto, () => true);
                break;
            }
            default: 
                super.onMessage(actionDto);
        }
    }

    watchReconnect(onChange: () => void) {
        const callback = new changesCallback<void>(onChange);

        this.allReconnectHandlers.push(callback);

        return new changeSubscription(() => {
            this.allReconnectHandlers.remove(callback);
        });
    }

    watchAllPerformanceHints(onChange: (e: Raven.Server.NotificationCenter.Notifications.PerformanceHint) => void) {
        const callback = new changesCallback<Raven.Server.NotificationCenter.Notifications.PerformanceHint>(onChange);

        this.allPerformanceHintsHandlers.push(callback);

        return new changeSubscription(() => {
            this.allPerformanceHintsHandlers.remove(callback);
        });
    }

    watchAllAlerts(onChange: (e: Raven.Server.NotificationCenter.Notifications.AlertRaised) => void) {
        const callback = new changesCallback<Raven.Server.NotificationCenter.Notifications.AlertRaised>(onChange);

        this.allAlertsHandlers.push(callback);

        return new changeSubscription(() => {
            this.allAlertsHandlers.remove(callback);
        });
    }

    watchAllOperations(onChange: (e: Raven.Server.NotificationCenter.Notifications.OperationChanged) => void) {
        const callback = new changesCallback<Raven.Server.NotificationCenter.Notifications.OperationChanged>(onChange);

        this.allOperationsHandlers.push(callback);

        return new changeSubscription(() => {
            this.allOperationsHandlers.remove(callback);
        });
    }

    watchOperation(operationId: number, onChange: (e: Raven.Server.NotificationCenter.Notifications.OperationChanged) => void) {
        const callback = new changesCallback<Raven.Server.NotificationCenter.Notifications.OperationChanged>(onChange);

        if (!this.watchedOperationsChanged.has(operationId)) {
            this.watchedOperationsChanged.set(operationId, ko.observableArray<changesCallback<Raven.Server.NotificationCenter.Notifications.OperationChanged>>());
        }

        const callbacks = this.watchedOperationsChanged.get(operationId);
        callbacks.push(callback);

        return new changeSubscription(() => {
            callbacks.remove(callback);
            if (callbacks().length === 0) {
                this.watchedOperationsChanged.delete(operationId);
            }
        });
    }

    watchAllNotificationUpdated(onChange: (e: Raven.Server.NotificationCenter.Notifications.NotificationUpdated) => void) {
        const callback = new changesCallback<Raven.Server.NotificationCenter.Notifications.NotificationUpdated>(onChange);

        this.allNotificationUpdatedHandlers.push(callback);

        return new changeSubscription(() => {
            this.allNotificationUpdatedHandlers.remove(callback);
        });
    }


}

export = abstractNotificationCenterClient;

