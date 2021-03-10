/// <reference path="../../../../typings/tsd.d.ts"/>

import clusterDashboard = require("../clusterDashboard");

abstract class widget<TData, TConfig = void> {
    static nextWidgetId = 1;

    controller: clusterDashboard;
    container: Element;

    fullscreen = ko.observable<boolean>(false);
    
    readonly nodes: string[];

    id: number;

    constructor(nodes: string[], controller: clusterDashboard) {
        this.id = widget.nextWidgetId++;
        this.controller = controller;
        this.nodes = nodes;
        
        this.fullscreen.subscribe(() => {
            this.controller.layout();
        })
    }
    
    attached(view: Element, container: Element) {
        this.container = container;
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
    
    abstract onData(nodeTag: string, data: TData): void;
}

export = widget;
