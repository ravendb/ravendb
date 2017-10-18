/// <reference path="../../typings/tsd.d.ts" />
import database = require("models/resources/database");
import abstractWebSocketClient = require("common/abstractWebSocketClient");
import d3 = require("d3");
import endpoints = require("endpoints");

class serverDashboardWebSocketClient extends abstractWebSocketClient<Raven.Server.Dashboard.AbstractDashboardNotification> {

    private readonly onData: (data: Raven.Server.Dashboard.AbstractDashboardNotification) => void;
    private static isoParser = d3.time.format.iso;
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

