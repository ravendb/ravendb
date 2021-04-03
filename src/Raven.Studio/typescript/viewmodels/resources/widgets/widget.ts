/// <reference path="../../../../typings/tsd.d.ts"/>

import clusterDashboardWebSocketClient = require("common/clusterDashboardWebSocketClient");
import clusterDashboard = require("viewmodels/resources/clusterDashboard");
import generalUtils = require("common/generalUtils");

abstract class widget<TConfig = unknown, TState = unknown> {
    static nextWidgetId = 1;
    
    static resizeAnimationDuration = 300;
    
    protected initialized: boolean = false;

    controller: clusterDashboard;
    container: HTMLElement;
    
    fullscreen = ko.observable<boolean>(false);

    syncUpdatesEnabled = false;
    firstSyncUpdateTaskId: number = -1;
    syncUpdateTaskId: number = -1;
    pendingUpdates = [] as Array<() => void>;
    
    configuredFor = ko.observableArray<clusterDashboardWebSocketClient>([]);

    id: number;

    constructor(controller: clusterDashboard, config: TConfig = undefined, state: TState = undefined) {
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
    
    attached(view: Element, container: HTMLElement) {
        this.container = container;
    }

    compositionComplete() {
        this.initialized = true;
        this.controller.onWidgetAdded(this);

        this.fullscreen.subscribe(
            () => setTimeout(
                () => this.afterComponentResized(), widget.resizeAnimationDuration));
    }

    abstract getType(): Raven.Server.Dashboard.Cluster.ClusterDashboardNotificationType;
    
    getConfiguration(): TConfig {
        return undefined;
    }
    
    getState(): TState {
        return undefined;
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
    
    restoreState(state: TState) {
        // empty by default
    }
    
    restoreConfiguration(config: TConfig) {
        // empty by default
    }
    
    dispose() {
        if (this.syncUpdateTaskId > 0) {
            clearInterval(this.syncUpdateTaskId);
            this.syncUpdateTaskId = -1;
        }
        
        if (this.firstSyncUpdateTaskId > 0) {
            clearTimeout(this.firstSyncUpdateTaskId);
            this.firstSyncUpdateTaskId = -1;
        }
    }

    static formatNumber(input: number): string {
        if (input === 0) {
            return "0";
        }
        if (input > 1) {
            return Math.round(input).toLocaleString();
        }
        return generalUtils.formatNumberToStringFixed(input, 2);
    }
}

export = widget;
