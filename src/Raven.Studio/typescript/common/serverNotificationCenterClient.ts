Topl/// <reference path="../../typings/tsd.d.ts" />
import changeSubscription = require("common/changeSubscription");
import changesCallback = require("common/changesCallback");
import endpoints = require("endpoints");

import abstractNotificationCenterClient = require("common/abstractNotificationCenterClient");

class serverNotificationCenterClient extends abstractNotificationCenterClient {

    protected allDatabaseChangedHandlers = ko.observableArray<changesCallback<Raven.Server.NotificationCenter.Notifications.Server.DatabaseChanged>>(); 
    protected watchedDatabaseChanged = new Map<string, KnockoutObservableArray<changesCallback<Raven.Server.NotificationCenter.Notifications.Server.DatabaseChanged>>>();
    protected watchedDatabaseChangedPrefixes = new Map<string, KnockoutObservableArray<changesCallback<Raven.Server.NotificationCenter.Notifications.Server.DatabaseChanged>>>();
    protected clusterTopologyChangedHandlers = ko.observableArray<changesCallback<Raven.Server.NotificationCenter.Notifications.Server.ClusterTopologyChanged>>(); 

    constructor() {
        super(null);
    }

    get connectionDescription() {
        return "Server Notification Center Client";
    }

    protected webSocketUrlFactory() {
        return endpoints.global.serverNotificationCenter.serverNotificationCenterWatch;
    }

    protected onMessage(actionDto: Raven.Server.NotificationCenter.Notifications.Notification) {
        const actionType = actionDto.Type;

        switch (actionType) {
            case "DatabaseChanged":
                const databaseDto = actionDto as Raven.Server.NotificationCenter.Notifications.Server.DatabaseChanged;
                this.fireEvents<Raven.Server.NotificationCenter.Notifications.Server.DatabaseChanged>(this.allDatabaseChangedHandlers(), databaseDto, () => true);

                this.watchedDatabaseChanged.forEach((callbacks, key) => {
                    this.fireEvents<Raven.Server.NotificationCenter.Notifications.Server.DatabaseChanged>(callbacks(), databaseDto, (event) => event.DatabaseName != null && event.DatabaseName === key);
                });

                this.watchedDatabaseChangedPrefixes.forEach((callbacks, key) => {
                    this.fireEvents<Raven.Server.NotificationCenter.Notifications.Server.DatabaseChanged>(callbacks(), databaseDto, (event) => event.DatabaseName != null && event.DatabaseName.startsWith(key));
                });
                break;

            case "ClusterTopologyChanged":
                const topologyChangedDto = actionDto as Raven.Server.NotificationCenter.Notifications.Server.ClusterTopologyChanged;
                this.fireEvents<Raven.Server.NotificationCenter.Notifications.Server.ClusterTopologyChanged>(this.clusterTopologyChangedHandlers(), topologyChangedDto, () => true);
                break;

            default:
                super.onMessage(actionDto);

        }
    }

    watchClusterTopologyChanges(onChange: (e: Raven.Server.NotificationCenter.Notifications.Server.ClusterTopologyChanged) => void) {
        const callback = new changesCallback<Raven.Server.NotificationCenter.Notifications.Server.ClusterTopologyChanged>(onChange);

        this.clusterTopologyChangedHandlers.push(callback);

        return new changeSubscription(() => {
            this.clusterTopologyChangedHandlers.remove(callback);
        });
    }

    watchAllDatabaseChanges(onChange: (e: Raven.Server.NotificationCenter.Notifications.Server.DatabaseChanged) => void) {
        const callback = new changesCallback<Raven.Server.NotificationCenter.Notifications.Server.DatabaseChanged>(onChange);

        this.allDatabaseChangedHandlers.push(callback);

        return new changeSubscription(() => {
            this.allDatabaseChangedHandlers.remove(callback);
        });
    }

    watchDatabaseChange(itemId: string, onChange: (e: Raven.Server.NotificationCenter.Notifications.Server.DatabaseChanged) => void): changeSubscription {
        const callback = new changesCallback<Raven.Server.NotificationCenter.Notifications.Server.DatabaseChanged>(onChange);

        if (!this.watchedDatabaseChanged.has(itemId)) {
            this.watchedDatabaseChanged.set(itemId, ko.observableArray<changesCallback<Raven.Server.NotificationCenter.Notifications.Server.DatabaseChanged>>());
        }

        const callbacks = this.watchedDatabaseChanged.get(itemId);
        callbacks.push(callback);

        return new changeSubscription(() => {
            callbacks.remove(callback);
            if (callbacks().length === 0) {
                this.watchedDatabaseChanged.delete(itemId);
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

