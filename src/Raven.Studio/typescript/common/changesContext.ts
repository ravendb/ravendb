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
import notificationCenter = require("common/notifications/notificationCenter");

class changesContext {
    static default = new changesContext();
    
    serverNotifications = ko.observable<serverNotificationCenterClient>();
    resourceNotifications = ko.observable<resourceNotificationCenterClient>();

    resourceChangesApi = ko.observable<changesApi>();
    afterChangesApiConnection = $.Deferred<changesApi>();

    private globalResourceSubscriptions: changeSubscription[] = [];

    constructor() {
        window.addEventListener("beforeunload", () => {
            this.disconnectFromResourceChangesApi("ChangingResource");
            this.serverNotifications().dispose();

            if (this.resourceNotifications()) {
                this.resourceNotifications().dispose();
            }
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
            this.disconnect("ChangingResource");
        }

        if (rs.disabled()) { //TODO: or not licensed
            this.navigateToResourceSpecificPage(rs);
            return;
        }

        const notificationsClient = new resourceNotificationCenterClient(rs);

        this.globalResourceSubscriptions.push(...notificationCenter.instance.configureForResource(notificationsClient));

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
            this.disconnect(cause);
        }
    }

    private disconnect(cause: resourceDisconnectionCause) {
        this.globalResourceSubscriptions.forEach(x => x.off());
        this.globalResourceSubscriptions = [];

        this.disconnectFromResourceChangesApi(cause);
        this.disconnectFromResourceNotificationCenter();
        notificationCenter.instance.resourceDisconnected();
    }
}

export = changesContext;
