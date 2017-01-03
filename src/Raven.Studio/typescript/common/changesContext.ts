/// <reference path="../../typings/tsd.d.ts" />

import changesApi = require("common/changesApi");
import resource = require("models/resources/resource");
import changeSubscription = require("common/changeSubscription");
import appUrl = require("common/appUrl");
import router = require("plugins/router");
import adminWatchClient = require("common/adminWatchClient");
import EVENTS = require("common/constants/events");

import resourceDisconnectedEventArgs = require("viewmodels/resources/resourceDisconnectedEventArgs");

class changesContext {
    static default = new changesContext();

    currentResourceChangesApi = ko.observable<changesApi>();
    globalChangesApi = ko.observable<adminWatchClient>();

    afterConnection = $.Deferred<changesApi>();
    afterConnectionResolved = false;

    sentSubscriptions: changeSubscription[];

    constructor() {
        window.addEventListener("beforeunload", () => {
            this.disconnectFromResourceChangesApi("ChangingResource");
            this.globalChangesApi().dispose();
        });

        this.currentResourceChangesApi.subscribe(newValue => {
            if (!newValue) {
                if (this.afterConnectionResolved) {
                    this.afterConnection = $.Deferred<changesApi>();
                    this.afterConnectionResolved = false;
                }
            } else {
                this.afterConnectionResolved = true;
                this.afterConnection.resolve(newValue);
            }
        });
    }

    connectGlobalChangesApi(): JQueryPromise<void> {
        const alreadyHasGlobalChangesApi = this.globalChangesApi();
        if (alreadyHasGlobalChangesApi) {
            return alreadyHasGlobalChangesApi.connectToWebSocketTask;
        }

        const globalChanges = new adminWatchClient();
        this.globalChangesApi(globalChanges);

        return globalChanges.connectToWebSocketTask;
    }

    updateChangesApi(rs: resource, subscriptions: (changes: changesApi) => changeSubscription[]) {
        const currentChanges = this.currentResourceChangesApi();
        if (currentChanges && currentChanges.getResource().qualifiedName === rs.qualifiedName) {
            // nothing to do - already connected to requested changes api
            return;
        }

        if (currentChanges) {
            this.disconnectFromResourceChangesApi("ChangingResource");
        }

        if (rs.disabled()) { //TODO: or not licensed
            this.navigateToResourceSpecificPage(rs);
            return;
        }

        const newChanges = new changesApi(rs);
        newChanges.connectToWebSocketTask.done(() => {
            this.currentResourceChangesApi(newChanges);

            this.sentSubscriptions = subscriptions(newChanges);

            this.navigateToResourceSpecificPage(rs);
        });
    }

    private navigateToResourceSpecificPage(rs: resource) {
        const locationHash = window.location.hash;
        const isMainPage = locationHash === appUrl.forResources();
        if (!isMainPage) {
            const updatedUrl = appUrl.forCurrentPage(rs);
            if (updatedUrl) {
                router.navigate(updatedUrl);
            }
        }
    }

    private disconnectFromResourceChangesApi(cause: resourceDisconnectionCause) {
        const currentChanges = this.currentResourceChangesApi();
        if (currentChanges) {
            this.sentSubscriptions.forEach(x => x.off());
            this.sentSubscriptions = [];
            currentChanges.dispose();
            this.currentResourceChangesApi(null);

            ko.postbox.publish(EVENTS.Resource.Disconnect, { resource: currentChanges.getResource(), cause: cause } as resourceDisconnectedEventArgs);
        }
    }

    disconnectIfCurrent(rs: resource, cause: resourceDisconnectionCause) {
        const currentChanges = this.currentResourceChangesApi();

        if (currentChanges && currentChanges.getResource().qualifiedName === rs.qualifiedName) {
            this.disconnectFromResourceChangesApi(cause);
        }
    }

}

export = changesContext;
