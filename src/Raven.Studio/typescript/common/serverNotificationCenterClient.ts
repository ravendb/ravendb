/// <reference path="../../typings/tsd.d.ts" />

import resource = require("models/resources/resource");
import changeSubscription = require("common/changeSubscription");
import changesCallback = require("common/changesCallback");
import EVENTS = require("common/constants/events");
import endpoints = require("endpoints");

import abstractNotificationCenterClient = require("common/abstractNotificationCenterClient");

class serverNotificationCenterClient extends abstractNotificationCenterClient {

    protected allResourceChangedHandlers = ko.observableArray<changesCallback<Raven.Server.NotificationCenter.Notifications.Server.ResourceChanged>>(); 
    protected watchedResourceChanged = new Map<string, KnockoutObservableArray<changesCallback<Raven.Server.NotificationCenter.Notifications.Server.ResourceChanged>>>();
    protected watchedResourceChangedPrefixes = new Map<string, KnockoutObservableArray<changesCallback<Raven.Server.NotificationCenter.Notifications.Server.ResourceChanged>>>();

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

    protected onMessage(actionDto: Raven.Server.NotificationCenter.Notifications.Server.ResourceChanged) {
        const actionType = actionDto.Type;

        switch (actionType) {
            case "ResourceChanged":
                const resourceDto = actionDto as Raven.Server.NotificationCenter.Notifications.Server.ResourceChanged;
                this.fireEvents<Raven.Server.NotificationCenter.Notifications.Server.ResourceChanged>(this.allResourceChangedHandlers(), resourceDto, () => true);

                this.watchedResourceChanged.forEach((callbacks, key) => {
                    this.fireEvents<Raven.Server.NotificationCenter.Notifications.Server.ResourceChanged>(callbacks(), resourceDto, (event) => event.ResourceName != null && event.ResourceName === key);
                });

                this.watchedResourceChangedPrefixes.forEach((callbacks, key) => {
                    this.fireEvents<Raven.Server.NotificationCenter.Notifications.Server.ResourceChanged>(callbacks(), resourceDto, (event) => event.ResourceName != null && event.ResourceName.startsWith(key));
                });
                break;
            default:
                super.onMessage(actionDto);

        }
    }

    watchAllResourceChanges(onChange: (e: Raven.Server.NotificationCenter.Notifications.Server.ResourceChanged) => void) {
        const callback = new changesCallback<Raven.Server.NotificationCenter.Notifications.Server.ResourceChanged>(onChange);

        this.allResourceChangedHandlers.push(callback);

        return new changeSubscription(() => {
            this.allResourceChangedHandlers.remove(callback);
        });
    }

    watchResourceChange(itemId: string, onChange: (e: Raven.Server.NotificationCenter.Notifications.Server.ResourceChanged) => void): changeSubscription {
        const callback = new changesCallback<Raven.Server.NotificationCenter.Notifications.Server.ResourceChanged>(onChange);

        if (!this.watchedResourceChanged.has(itemId)) {
            this.watchedResourceChanged.set(itemId, ko.observableArray<changesCallback<Raven.Server.NotificationCenter.Notifications.Server.ResourceChanged>>());
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

    watchResourceChangeStartingWith(itemIdPrefix: string, onChange: (e: Raven.Server.NotificationCenter.Notifications.Server.ResourceChanged) => void): changeSubscription {
        const callback = new changesCallback<Raven.Server.NotificationCenter.Notifications.Server.ResourceChanged>(onChange);

        if (!this.watchedResourceChangedPrefixes.has(itemIdPrefix)) {
            this.watchedResourceChangedPrefixes.set(itemIdPrefix, ko.observableArray<changesCallback<Raven.Server.NotificationCenter.Notifications.Server.ResourceChanged>>());
        }

        const callbacks = this.watchedResourceChangedPrefixes.get(itemIdPrefix);
        callbacks.push(callback);

        return new changeSubscription(() => {
            callbacks.remove(callback);
            if (callbacks().length === 0) {
                this.watchedResourceChangedPrefixes.delete(itemIdPrefix);
            }
        });
    }

}

export = serverNotificationCenterClient;

