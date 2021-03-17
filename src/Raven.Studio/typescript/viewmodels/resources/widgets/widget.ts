/// <reference path="../../../../typings/tsd.d.ts"/>

import clusterDashboardWebSocketClient = require("common/clusterDashboardWebSocketClient");
import clusterDashboard = require("viewmodels/resources/clusterDashboard");

abstract class widget<TData, TConfig = void> {
    static nextWidgetId = 1;
    
    static resizeAnimationDuration = 300;
    
    private initialized: boolean = false;

    controller: clusterDashboard;
    container: Element;
    
    fullscreen = ko.observable<boolean>(false);

    syncUpdatesEnabled = false;
    firstSyncUpdateTaskId: number = -1;
    syncUpdateTaskId: number = -1;
    pendingUpdates = [] as Array<() => void>;
    
    configuredFor = ko.observableArray<clusterDashboardWebSocketClient>([]);

    id: number;

    constructor(controller: clusterDashboard) {
        this.id = widget.nextWidgetId++;
        this.controller = controller;
        
        this.fullscreen.subscribe(() => {
            this.controller.layout();
        });
    }
    
    scheduleSyncUpdate(action: () => void) {
        if (!this.syncUpdatesEnabled) {
            throw new Error("Sync updates wasn't enabled for this widget");
        }
        this.pendingUpdates.push(action);
    }
    
    protected enableSyncUpdates() {
        // align first execution to second
        const firstInvocationIn = new Date().getTime() % 1000;
        this.firstSyncUpdateTaskId = setTimeout(() => {
            this.syncUpdateTaskId = setInterval(() => this.syncUpdate(), 1000);
        }, firstInvocationIn);
        this.syncUpdatesEnabled = true;
    }
    
    attached(view: Element, container: Element) {
        this.container = container;
    }

    compositionComplete() {
        this.initialized = true;
        this.controller.onWidgetReady(this);

        this.fullscreen.subscribe(
            () => setTimeout(
                () => this.afterComponentResized(), widget.resizeAnimationDuration));
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
    
    private syncUpdate() {
        const updatesCount = this.pendingUpdates.length;
        this.pendingUpdates.forEach(action => action());
        this.pendingUpdates = [];
        this.afterSyncUpdate(updatesCount);
    }
    
    protected afterSyncUpdate(updatesCount: number) {
        // empty by default
    }
    
    protected afterComponentResized() {
        // empty by default
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
        if (this.syncUpdateTaskId > 0) {
            clearInterval(this.syncUpdateTaskId);
            this.syncUpdateTaskId = -1;
        }
        
        if (this.firstSyncUpdateTaskId > 0) {
            clearTimeout(this.firstSyncUpdateTaskId);
            this.firstSyncUpdateTaskId = -1;
        }
        
        for (const ws of this.configuredFor()) {
            ws.sendCommand({
                Command: "unwatch",
                Id: this.id
            } as Raven.Server.ClusterDashboard.WidgetRequest);
        }
    }
}

export = widget;
