/// <reference path="../../../../typings/tsd.d.ts"/>

import clusterDashboard = require("viewmodels/resources/clusterDashboard");
import clusterDashboardWebSocketClient = require("common/clusterDashboardWebSocketClient");

interface nodeConnectionStatus {
    nodeTag: string;
    connected: KnockoutObservable<boolean>;
} 

abstract class widget<TData, TConfig = void> {
    static nextWidgetId = 1;

    controller: clusterDashboard;
    container: Element;

    fullscreen = ko.observable<boolean>(false);
    
    readonly nodesStatus = ko.observableArray<nodeConnectionStatus>();
    
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
        this.controller.layoutNewWidget(this);
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
        if (this.supportedOnNode(ws.nodeTag, this.controller.currentServerNodeTag)) {
            ws.sendCommand({
                Command: "watch",
                Config: this.getConfiguration(),
                Id: this.id,
                Type: this.getType()
            });
            
            this.configuredFor.push(ws);
        }
    }
    
    onClientDisconnected(ws: clusterDashboardWebSocketClient) {
        this.configuredFor.remove(ws);
        
        //TODO: 
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
