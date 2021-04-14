/// <reference path="../../typings/tsd.d.ts" />
import abstractWebSocketClient = require("common/abstractWebSocketClient");
import endpoints = require("endpoints");

class clusterDashboardWebSocketClient extends abstractWebSocketClient<Raven.Server.Dashboard.Cluster.WidgetMessage> {

    readonly nodeTag: string;
    private readonly onData: (data: Raven.Server.Dashboard.Cluster.WidgetMessage) => void;
    loading = ko.observable<boolean>(true);
    private readonly onConnection: () => void;
    private readonly onDisconnect: () => void;

    constructor(nodeTag: string, onData: (data: Raven.Server.Dashboard.Cluster.WidgetMessage) => void, onConnection: () => void, onDisconnect: () => void) {
        super(null);
        this.nodeTag = nodeTag;
        this.onData = onData;
        this.onConnection = onConnection;
        this.onDisconnect = onDisconnect;
        
        this.isConnected.subscribe(connected => {
            if (connected) {
                this.onConnection();
            } else {
                this.onDisconnect();
            }
        });
    }
    
    public sendCommand(data: Raven.Server.Dashboard.Cluster.WidgetRequest) {
        const payload = JSON.stringify(data, null, 0);
        this.webSocket.send(payload);
    }

    get connectionDescription() {
        return "Cluster Dashboard";
    }

    protected webSocketUrlFactory() {
        return endpoints.global.clusterDashboard.clusterDashboardWatch + "?node=" + this.nodeTag;
    }

    get autoReconnect() {
        return true;
    }

    protected onHeartBeat() {
        this.loading(false);
    }

    protected onMessage(e: Raven.Server.Dashboard.Cluster.WidgetMessage) {
        this.loading(false);

        this.onData(e);
    }
}

export = clusterDashboardWebSocketClient;

