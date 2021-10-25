import widget = require("viewmodels/resources/widgets/widget");
import clusterDashboardWebSocketClient = require("common/clusterDashboardWebSocketClient");

abstract class websocketBasedWidget<TData, TConfig = unknown, TState = unknown> extends widget<TConfig, TState> {

    abstract view: { default: string };
    
    getView() {
        if (!this.view) {
            throw new Error("Looks like you forgot to define view in: " + this.constructor.name);
        }
        if (!this.view.default.trim().startsWith("<")) {
            console.warn("View doesn't start with '<'");
        }
        return this.view.default || this.view;
    }
    
    abstract onData(nodeTag: string, data: TData): void;

    supportedOnNode(targetNodeTag: string, currentServerNodeTag: string): boolean {
        return true;
    }

    abstract getType(): Raven.Server.Dashboard.Cluster.ClusterDashboardNotificationType;
    
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
                Id: this.id,
                Type: undefined,
                Config: undefined
            });
        }
    }
}

export = websocketBasedWidget;
