/// <reference path="../../typings/tsd.d.ts" />

import resource = require("models/resources/resource");
import changeSubscription = require("common/changeSubscription");
import changesCallback = require("common/changesCallback");

import abstractWebSocketClient = require("common/abstractWebSocketClient");

import globalAlertNotification = Raven.Server.Alerts.GlobalAlertNotification;

/**
 * A.K.A. Global Changes API
 */
class adminWatchClient extends abstractWebSocketClient {

    constructor() {
        super(null);
    }

    private allAlertsHandlers = ko.observableArray<changesCallback<globalAlertNotification>>();
    private allItemsHandlers = ko.observableArray<changesCallback<globalAlertNotification>>();

    private watchedItems = new Map<string, KnockoutObservableArray<changesCallback<globalAlertNotification>>>();
    private watchedItemPrefixes = new Map<string, KnockoutObservableArray<changesCallback<globalAlertNotification>>>();

    get connectionDescription() {
        return "Global Changes API";
    }

    protected webSocketUrlFactory(token: singleAuthToken) {
        const connectionString = "singleUseAuthToken=" + token.Token;
        return "/admin/watch?" + connectionString;
    }

    protected onOpen() {
        super.onOpen();
        this.connectToWebSocketTask.resolve();
    }

    protected onMessage(e: any) {
        if (!e.data.trim()) {
            // it is heartbeat only
            return;
        }

        const eventDto: globalAlertNotification = JSON.parse(e.data);
        const eventOperation = eventDto.Operation;

        switch (eventOperation) {
            case "Write":
            case "Delete":
                this.fireEvents<globalAlertNotification>(this.allItemsHandlers(), eventDto, () => true);

                this.watchedItems.forEach((callbacks, key) => {
                    this.fireEvents<globalAlertNotification>(callbacks(), eventDto, (event) => event.Id != null && event.Id === key);
                });

                this.watchedItemPrefixes.forEach((callbacks, key) => {
                    this.fireEvents<globalAlertNotification>(callbacks(), eventDto, (event) => event.Id != null && event.Id.startsWith(key));
                });
                break;

            case "AlertRaised":
            case "AlertDeleted":
                this.fireEvents<globalAlertNotification>(this.allAlertsHandlers(), eventDto, () => true);
                break;
            default:
                console.log("Unhandled Changes API notification type: " + eventDto.Id);
        }
    }

    watchAlerts(onChange: (e: globalAlertNotification) => void) {
        const callback = new changesCallback<globalAlertNotification>(onChange);

        this.allAlertsHandlers.push(callback);

        return new changeSubscription(() => {
            this.allAlertsHandlers.remove(callback);
        });
    }

    watchAllItems(onChange: (e: globalAlertNotification) => void) {
        var callback = new changesCallback<globalAlertNotification>(onChange);

        this.allItemsHandlers.push(callback);

        return new changeSubscription(() => {
            this.allItemsHandlers.remove(callback);
        });
    }

    watchItem(itemId: string, onChange: (e: globalAlertNotification) => void): changeSubscription {
        const callback = new changesCallback<globalAlertNotification>(onChange);

        if (!this.watchedItems.has(itemId)) {
            this.watchedItems.set(itemId, ko.observableArray<changesCallback<globalAlertNotification>>());
        }

        const callbacks = this.watchedItems.get(itemId);
        callbacks.push(callback);

        return new changeSubscription(() => {
            callbacks.remove(callback);
            if (callbacks().length === 0) {
                this.watchedItems.delete(itemId);
            }
        });
    }

    watchItemsStartingWith(itemIdPrefix: string, onChange: (e: globalAlertNotification) => void): changeSubscription {
        const callback = new changesCallback<globalAlertNotification>(onChange);

        if (!this.watchedItemPrefixes.has(itemIdPrefix)) {
            this.watchedItemPrefixes.set(itemIdPrefix, ko.observableArray<changesCallback<globalAlertNotification>>());
        }

        const callbacks = this.watchedItemPrefixes.get(itemIdPrefix);
        callbacks.push(callback);

        return new changeSubscription(() => {
            callbacks.remove(callback);
            if (callbacks().length === 0) {
                this.watchedItemPrefixes.delete(itemIdPrefix);
            }
        });
    }
}

export = adminWatchClient;

