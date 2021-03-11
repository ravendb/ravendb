/// <reference path="../../typings/tsd.d.ts" />
import abstractWebSocketClient = require("common/abstractWebSocketClient");
import endpoints = require("endpoints");

class clusterDashboardWebSocketClient extends abstractWebSocketClient<Raven.Server.ClusterDashboard.WidgetMessage> {

    readonly nodeTag: string;
    private readonly onData: (data: Raven.Server.ClusterDashboard.WidgetMessage) => void;
    loading = ko.observable<boolean>(true);
    private readonly onConnection: () => void;
    private readonly onDisconnect: () => void;

    constructor(nodeTag: string, onData: (data: Raven.Server.ClusterDashboard.WidgetMessage) => void, onConnection: () => void, onDisconnect: () => void) {
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

