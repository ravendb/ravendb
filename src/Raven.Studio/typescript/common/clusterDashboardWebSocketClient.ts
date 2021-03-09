/// <reference path="../../typings/tsd.d.ts" />
import abstractWebSocketClient = require("common/abstractWebSocketClient");
import endpoints = require("endpoints");

class clusterDashboardWebSocketClient extends abstractWebSocketClient<Raven.Server.ClusterDashboard.WidgetMessage> {

    private readonly nodeTag: string;
    private readonly onData: (data: Raven.Server.ClusterDashboard.WidgetMessage) => void;
    loading = ko.observable<boolean>(true);

    constructor(nodeTag: string, onData: (data: Raven.Server.ClusterDashboard.WidgetMessage) => void) {
        super(null);
        this.nodeTag = nodeTag;
        this.onData = onData;
    }
    
    public sendCommand(data: Raven.Server.ClusterDashboard.WidgetRequest) {
        const payload = JSON.stringify(data, null, 0);
        this.webSocket.send(payload);
    }

    get connectionDescription() {
        return "Cluster Dashboard";
    }

    protected webSocketUrlFactory() {
        return endpoints.global.clusterDashboard.clusterDashboardWatch;
    }

    get autoReconnect() {
        // we want to manually reconnect and configure new connection
        return false;
    }

    protected onHeartBeat() {
        this.loading(false);
    }

    protected onMessage(e: Raven.Server.ClusterDashboard.WidgetMessage) {
        this.loading(false);

        this.onData(e);
    }
}

export = clusterDashboardWebSocketClient;

