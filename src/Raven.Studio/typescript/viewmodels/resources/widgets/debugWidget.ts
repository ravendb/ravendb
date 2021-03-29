import websocketBasedWidget = require("viewmodels/resources/widgets/websocketBasedWidget");
import clusterDashboardWebSocketClient = require("common/clusterDashboardWebSocketClient");

class debugWidget extends websocketBasedWidget<any> {

    log = ko.observableArray<string>();

    getType(): Raven.Server.Dashboard.Cluster.ClusterDashboardNotificationType {
        return "Debug";
    }

    onData(nodeTag: string, data: any) {
        console.log("DEBUG nodetag = " + nodeTag + ", data = ", data);
        //TODO: console.log("cpu data = ", nodeTag, data);
    }
    
    onClientConnected(ws: clusterDashboardWebSocketClient) {
        super.onClientConnected(ws);
        
        this.log.push(new Date() + " On connected = " + ws.nodeTag);
    }
    
    onClientDisconnected(ws: clusterDashboardWebSocketClient) {
        super.onClientDisconnected(ws);

        this.log.push(new Date() + " On disconnected = " + ws.nodeTag);
    }
}

export = debugWidget;
