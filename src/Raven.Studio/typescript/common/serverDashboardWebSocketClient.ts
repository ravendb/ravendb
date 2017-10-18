/// <reference path="../../typings/tsd.d.ts" />
import abstractWebSocketClient = require("common/abstractWebSocketClient");
import endpoints = require("endpoints");

class serverDashboardWebSocketClient extends abstractWebSocketClient<Raven.Server.Dashboard.AbstractDashboardNotification> {

    private readonly onData: (data: Raven.Server.Dashboard.AbstractDashboardNotification) => void;
    loading = ko.observable<boolean>(true);

    constructor(onData: (data: Raven.Server.Dashboard.AbstractDashboardNotification) => void) {
        super(null);
        this.onData = onData;
    }

    get connectionDescription() {
        return "Server Dashboard";
    }

    protected webSocketUrlFactory() {
        return endpoints.global.serverDashboard.serverDashboardWatch;
    }

    get autoReconnect() {
        return false;
    }

    protected onHeartBeat() {
        this.loading(false);
    }

    protected onMessage(e: Raven.Server.Dashboard.AbstractDashboardNotification) {
        this.loading(false);

        this.onData(e);
    }
}

export = serverDashboardWebSocketClient;

