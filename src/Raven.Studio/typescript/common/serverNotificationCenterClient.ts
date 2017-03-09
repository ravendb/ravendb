/// <reference path="../../typings/tsd.d.ts" />

import database = require("models/resources/database");
import changeSubscription = require("common/changeSubscription");
import changesCallback = require("common/changesCallback");
import EVENTS = require("common/constants/events");
import endpoints = require("endpoints");

import abstractNotificationCenterClient = require("common/abstractNotificationCenterClient");

class serverNotificationCenterClient extends abstractNotificationCenterClient {

    protected allResourceChangedHandlers = ko.observableArray<changesCallback<Raven.Server.NotificationCenter.Notifications.Server.DatabaseChanged>>(); 
    protected watchedResourceChanged = new Map<string, KnockoutObservableArray<changesCallback<Raven.Server.NotificationCenter.Notifications.Server.DatabaseChanged>>>();
    protected watchedDatabaseChangedPrefixes = new Map<string, KnockoutObservableArray<changesCallback<Raven.Server.NotificationCenter.Notifications.Server.DatabaseChanged>>>();

    constructor() {
        super(null);
    }

    get connectionDescription() {
        return "Server Notification Center Client";
    }

    protected webSocketUrlFactory(token: singleAuthToken) {
        const connectionString = "?singleUseAuthToken=" + token.Token;
        return endpoints.global.serverNotificationCenter.notificationCenterWatch + connectionString;
    }

    protected onMessage(actionDto: Raven.Server.NotificationCenter.Notifications.Server.DatabaseChanged) {
        const actionType = actionDto.Type;

        switch (actionType) {
            case "DatabaseChanged":
                const resourceDto = actionDto as Raven.Server.NotificationCenter.Notifications.Server.DatabaseChanged;
                this.fireEvents<Raven.Server.NotificationCenter.Notifications.Server.DatabaseChanged>(this.allResourceChangedHandlers(), resourceDto, () => true);

                this.watchedResourceChanged.forEach((callbacks, key) => {
                    this.fireEvents<Raven.Server.NotificationCenter.Notifications.Server.DatabaseChanged>(callbacks(), resourceDto, (event) => event.DatabaseName != null && event.DatabaseName === key);
                });

                this.watchedDatabaseChangedPrefixes.forEach((callbacks, key) => {
                    this.fireEvents<Raven.Server.NotificationCenter.Notifications.Server.DatabaseChanged>(callbacks(), resourceDto, (event) => event.DatabaseName != null && event.DatabaseName.startsWith(key));
                });
                break;
            default:
                super.onMessage(actionDto);

        }
    }

    watchAllResourceChanges(onChange: (e: Raven.Server.NotificationCenter.Notifications.Server.DatabaseChanged) => void) {
        const callback = new changesCallback<Raven.Server.NotificationCenter.Notifications.Server.DatabaseChanged>(onChange);

        this.allResourceChangedHandlers.push(callback);

        return new changeSubscription(() => {
            this.allResourceChangedHandlers.remove(callback);
        });
    }

    watchDatabaseChange(itemId: string, onChange: (e: Raven.Server.NotificationCenter.Notifications.Server.DatabaseChanged) => void): changeSubscription {
        const callback = new changesCallback<Raven.Server.NotificationCenter.Notifications.Server.DatabaseChanged>(onChange);

        if (!this.watchedResourceChanged.has(itemId)) {
            this.watchedResourceChanged.set(itemId, ko.observableArray<changesCallback<Raven.Server.NotificationCenter.Notifications.Server.DatabaseChanged>>());
        }

        const callbacks = this.watchedResourceChanged.get(itemId);
        callbacks.push(callback);

        return new changeSubscription(() => {
            callbacks.remove(callback);
            if (callbacks().length === 0) {
                this.watchedResourceChanged.delete(itemId);
            }
        });
    }

    watchDatabaseChangeStartingWith(itemIdPrefix: string, onChange: (e: Raven.Server.NotificationCenter.Notifications.Server.DatabaseChanged) => void): changeSubscription {
        const callback = new changesCallback<Raven.Server.NotificationCenter.Notifications.Server.DatabaseChanged>(onChange);

        if (!this.watchedDatabaseChangedPrefixes.has(itemIdPrefix)) {
            this.watchedDatabaseChangedPrefixes.set(itemIdPrefix, ko.observableArray<changesCallback<Raven.Server.NotificationCenter.Notifications.Server.DatabaseChanged>>());
        }

        const callbacks = this.watchedDatabaseChangedPrefixes.get(itemIdPrefix);
        callbacks.push(callback);

        return new changeSubscription(() => {
            callbacks.remove(callback);
            if (callbacks().length === 0) {
                this.watchedDatabaseChangedPrefixes.delete(itemIdPrefix);
            }
        });
    }

}

export = serverNotificationCenterClient;

