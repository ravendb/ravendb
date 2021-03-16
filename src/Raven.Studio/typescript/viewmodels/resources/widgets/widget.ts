/// <reference path="../../../../typings/tsd.d.ts"/>

import clusterDashboardWebSocketClient = require("common/clusterDashboardWebSocketClient");
import clusterDashboard = require("viewmodels/resources/clusterDashboard");

abstract class widget<TData, TConfig = void> {
    static nextWidgetId = 1;
    
    private initialized: boolean = false;

    controller: clusterDashboard;
    container: Element;

    fullscreen = ko.observable<boolean>(false);
    
    configuredFor = ko.observableArray<clusterDashboardWebSocketClient>([]);

    id: number;

    constructor(controller: clusterDashboard) {
        this.id = widget.nextWidgetId++;
        this.controller = controller;
        
        this.fullscreen.subscribe(() => {
            this.controller.layout();
        });
    }
    
    attached(view: Element, container: Element) {
        this.container = container;
    }

    compositionComplete() {
        this.initialized = true;
        this.controller.onWidgetReady(this);
    }

    abstract getType(): Raven.Server.ClusterDashboard.WidgetType;
    
    getConfiguration(): TConfig {
        return undefined;
    }
    
    supportedOnNode(targetNodeTag: string, currentServerNodeTag: string): boolean {
        return true;
    }
    
    toggleFullscreen() {
        this.fullscreen(!this.fullscreen());
    }
    
    remove() {
        this.controller.deleteWidget(this);
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
    
    abstract onData(nodeTag: string, data: TData): void;
    
    dispose() {
        for (const ws of this.configuredFor()) {
            ws.sendCommand({
                Command: "unwatch",
                Id: this.id
            } as Raven.Server.ClusterDashboard.WidgetRequest);
        }
    }
}

export = widget;
