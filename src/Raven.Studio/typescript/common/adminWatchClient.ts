/// <reference path="../../typings/tsd.d.ts" />

import resource = require("models/resources/resource");
import changeSubscription = require("common/changeSubscription");
import changesCallback = require("common/changesCallback");

import abstractWebSocketClient = require("common/abstractWebSocketClient");

/**
 * A.K.A. Global Changes API
 */
class adminWatchClient extends abstractWebSocketClient {

    constructor() {
        super(null);
    }

    private allAlertsHandlers = ko.observableArray<changesCallback<adminWatchMessage>>();
    private allItemsHandlers = ko.observableArray<changesCallback<adminWatchMessage>>();

    private watchedItems = new Map<string, KnockoutObservableArray<changesCallback<adminWatchMessage>>>();
    private watchedItemPrefixes = new Map<string, KnockoutObservableArray<changesCallback<adminWatchMessage>>>();

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
        const eventDto: adminWatchMessage = JSON.parse(e.data);
        const eventOperation = eventDto.Operation;

        switch (eventOperation) {
            case "Write":
            case "Delete":
                this.fireEvents<adminWatchMessage>(this.allItemsHandlers(), eventDto, () => true);

                this.watchedItems.forEach((callbacks, key) => {
                    this.fireEvents<adminWatchMessage>(callbacks(), eventDto, (event) => event.Id != null && event.Id === key);
                });

                this.watchedItemPrefixes.forEach((callbacks, key) => {
                    this.fireEvents<adminWatchMessage>(callbacks(), eventDto, (event) => event.Id != null && event.Id.startsWith(key));
                });
                break;

            case "AlertRaised":
            case "AlertDeleted":
                this.fireEvents<adminWatchMessage>(this.allAlertsHandlers(), eventDto, () => true);
                break;
            default:
                console.log("Unhandled Changes API notification type: " + eventDto.Id);
        }
    }

    watchAlerts(onChange: (e: adminWatchMessage) => void) {
        const callback = new changesCallback<adminWatchMessage>(onChange);

        this.allAlertsHandlers.push(callback);

        return new changeSubscription(() => {
            this.allAlertsHandlers.remove(callback);
        });
    }

    watchAllItems(onChange: (e: adminWatchMessage) => void) {
        var callback = new changesCallback<adminWatchMessage>(onChange);

        this.allItemsHandlers.push(callback);

        return new changeSubscription(() => {
            this.allItemsHandlers.remove(callback);
        });
    }

    watchItem(itemId: string, onChange: (e: adminWatchMessage) => void): changeSubscription {
        const callback = new changesCallback<adminWatchMessage>(onChange);

        if (!this.watchedItems.has(itemId)) {
            this.watchedItems.set(itemId, ko.observableArray<changesCallback<adminWatchMessage>>());
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

    watchItemsStartingWith(itemIdPrefix: string, onChange: (e: adminWatchMessage) => void): changeSubscription {
        const callback = new changesCallback<adminWatchMessage>(onChange);

        if (!this.watchedItemPrefixes.has(itemIdPrefix)) {
            this.watchedItemPrefixes.set(itemIdPrefix, ko.observableArray<changesCallback<adminWatchMessage>>());
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

