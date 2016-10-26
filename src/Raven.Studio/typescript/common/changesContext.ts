/// <reference path="../../typings/tsd.d.ts" />

import changesApi = require("common/changesApi");
import resource = require("models/resources/resource");
import changeSubscription = require("common/changeSubscription");
import appUrl = require("common/appUrl");
import router = require("plugins/router");

class changesContext {
    static default = new changesContext();

    currentResourceChangesApi = ko.observable<changesApi>(); //TODO: make private and use callback with changes as parameter
    globalChangesApi = ko.observable<changesApi>();

    afterConnection = $.Deferred<changesApi>();
    afterConnectionResolved = false;

    sentSubscriptions: changeSubscription[];

    constructor() {
        window.addEventListener("beforeunload", () => {
            this.disconnectFromResourceChangesApi();
            //TODO: disconnect global changes
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
        const globalChanges = new changesApi(null);
        this.globalChangesApi(globalChanges);

        return globalChanges.connectToChangesApiTask;
    }

    updateChangesApi(rs: resource, subscriptions: (changes: changesApi) => changeSubscription[]) {
        const currentChanges = this.currentResourceChangesApi();
        if (currentChanges && currentChanges.getResource().qualifiedName === rs.qualifiedName) {
            // nothing to do - already connected to requested changes api
            return;
        }

        if (currentChanges) {
            this.disconnectFromResourceChangesApi();
        }

        if (rs.disabled) { //TODO: or not licensed
            this.navigateToResourceSpecificPage(rs);
            return;
        }

        const newChanges = new changesApi(rs);
        newChanges.connectToChangesApiTask.done(() => {
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

    disconnectFromResourceChangesApi() {
        const currentChanges = this.currentResourceChangesApi();
        if (currentChanges) {
            this.sentSubscriptions.forEach(x => x.off());
            this.sentSubscriptions = [];
            currentChanges.dispose();
            this.currentResourceChangesApi(null);
        }
    }

    disconnectIfCurrent(rs: resource) {
        const currentChanges = this.currentResourceChangesApi();

        if (currentChanges && currentChanges.getResource().qualifiedName === rs.qualifiedName) {
            this.disconnectFromResourceChangesApi();
        }
    }

}

export = changesContext;
