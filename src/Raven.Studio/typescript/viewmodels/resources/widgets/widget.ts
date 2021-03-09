

abstract class widget<TData, TConfig = void> {
    static nextWidgetId = 1;
    
    readonly nodes: string[];

    id: number;

    constructor(nodes: string[]) {
        this.id = widget.nextWidgetId++;
        this.nodes = nodes;
    }
    
    abstract getType(): Raven.Server.ClusterDashboard.WidgetType;
    
    getConfiguration(): TConfig {
        return undefined;
    }
    
    abstract onData(nodeTag: string, data: TData): void;
}

export = widget;
