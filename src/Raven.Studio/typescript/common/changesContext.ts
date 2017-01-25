/// <reference path="../../typings/tsd.d.ts" />

import changesApi = require("common/changesApi");
import resource = require("models/resources/resource");
import changeSubscription = require("common/changeSubscription");
import appUrl = require("common/appUrl");
import router = require("plugins/router");
import serverNotificationCenterClient = require("common/serverNotificationCenterClient");
import resourceNotificationCenterClient = require("common/resourceNotificationCenterClient");
import EVENTS = require("common/constants/events");

import resourceDisconnectedEventArgs = require("viewmodels/resources/resourceDisconnectedEventArgs");

class changesContext {
    static default = new changesContext();
    
    serverNotifications = ko.observable<serverNotificationCenterClient>();

    resourceChangesApi = ko.observable<changesApi>();
    afterChangesApiConnection = $.Deferred<changesApi>();

    resourceNotifications = ko.observable<resourceNotificationCenterClient>();

    private globalResourceSubscriptions: changeSubscription[] = [];

    constructor() {
        window.addEventListener("beforeunload", () => {
            this.disconnectFromResourceChangesApi("ChangingResource");
            //TODO: disconnect from notification center?
            this.serverNotifications().dispose();
        });

        this.resourceChangesApi.subscribe(newValue => {
            if (!newValue) {
                if (this.afterChangesApiConnection.state() === "resolved") {
                    this.afterChangesApiConnection = $.Deferred<changesApi>();
                }
            } else {
                this.afterChangesApiConnection.resolve(newValue);
            }
        });
    }

    connectServerWideNotificationCenter(): JQueryPromise<void> {
        const alreadyHasGlobalChangesApi = this.serverNotifications();
        if (alreadyHasGlobalChangesApi) {
            return alreadyHasGlobalChangesApi.connectToWebSocketTask;
        }

        const serverClient = new serverNotificationCenterClient();
        this.serverNotifications(serverClient);

        return serverClient.connectToWebSocketTask;
    }

    changeResource(rs: resource, globalResourceChangesApiSubscriptions?: (changes: changesApi) => changeSubscription[],
        globalResourceNotificationCenterSubscriptions?: (resourceNotificationCenterClient: resourceNotificationCenterClient) => changeSubscription[]): void {
        const currentChanges = this.resourceChangesApi();
        if (currentChanges && currentChanges.getResource().qualifiedName === rs.qualifiedName) {
            // nothing to do - already connected to requested changes api
            return;
        }

        if (currentChanges) {
            this.globalResourceSubscriptions.forEach(x => x.off());
            this.globalResourceSubscriptions = [];

            this.disconnectFromResourceChangesApi("ChangingResource");
            this.disconnectFromResourceNotificationCenter();
        }

        if (rs.disabled()) { //TODO: or not licensed
            this.navigateToResourceSpecificPage(rs);
            return;
        }

        const notificationsClient = new resourceNotificationCenterClient(rs);
        if (globalResourceNotificationCenterSubscriptions) {
            this.globalResourceSubscriptions.push(...globalResourceNotificationCenterSubscriptions(notificationsClient));
        }

        const newChanges = new changesApi(rs);
        newChanges.connectToWebSocketTask.done(() => {
            this.resourceChangesApi(newChanges);

            if (globalResourceChangesApiSubscriptions) {
                this.globalResourceSubscriptions.push(...globalResourceChangesApiSubscriptions(newChanges));
            }

            this.navigateToResourceSpecificPage(rs);
        });

        
        this.resourceNotifications(notificationsClient);
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

    private disconnectFromResourceNotificationCenter() {
        const currentClient = this.resourceNotifications();
        if (currentClient) {
            currentClient.dispose();
            this.resourceNotifications(null);
        }
    }

    private disconnectFromResourceChangesApi(cause: resourceDisconnectionCause) {
        const currentChanges = this.resourceChangesApi();
        if (currentChanges) {
            currentChanges.dispose();
            this.resourceChangesApi(null);

            ko.postbox.publish(EVENTS.Resource.Disconnect, { resource: currentChanges.getResource(), cause: cause } as resourceDisconnectedEventArgs);
        }
    }

    disconnectIfCurrent(rs: resource, cause: resourceDisconnectionCause) {
        const currentChanges = this.resourceChangesApi();

        if (currentChanges && currentChanges.getResource().qualifiedName === rs.qualifiedName) {
            this.disconnectFromResourceChangesApi(cause);
            this.disconnectFromResourceNotificationCenter();
        }
    }

}

export = changesContext;
