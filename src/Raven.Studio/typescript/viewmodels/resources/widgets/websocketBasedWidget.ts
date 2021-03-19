import widget = require("viewmodels/resources/widgets/widget");
import clusterDashboardWebSocketClient = require("common/clusterDashboardWebSocketClient");

abstract class websocketBasedWidget<TData, TConfig = unknown, TState = unknown> extends widget<TConfig, TState> {
    abstract onData(nodeTag: string, data: TData): void;

    supportedOnNode(targetNodeTag: string, currentServerNodeTag: string): boolean {
        return true;
    }
    
    onClientConnected(ws: clusterDashboardWebSocketClient) {
        if (!this.initialized) {
            // ignore calls when widget is not yet ready - we send command after compositionComplete
            return;
        }

        if (this.supportedOnNode(ws.nodeTag, this.controller.currentServerNodeTag)) {
            const command = this.createWatchCommand();
            if (command) {
                ws.sendCommand(command);

                this.configuredFor.push(ws);
            }
        }
    }

    protected createWatchCommand() {
        return {
            Command: "watch",
            Config: this.getConfiguration(),
            Id: this.id,
            Type: this.getType()
        };
    }

    onClientDisconnected(ws: clusterDashboardWebSocketClient) {
        if (!this.initialized) {
            // ignore calls when widget is not yet ready - we send command after compositionComplete
            return;
        }

        this.configuredFor.remove(ws);
    }
    
    dispose() {
        super.dispose();
        for (const ws of this.configuredFor()) {
            ws.sendCommand({
                Command: "unwatch",
                Id: this.id
            } as Raven.Server.ClusterDashboard.WidgetRequest);
        }
    }

}

export = websocketBasedWidget;
