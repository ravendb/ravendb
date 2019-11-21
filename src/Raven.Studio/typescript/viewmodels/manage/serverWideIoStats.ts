import viewModelBase = require("viewmodels/viewModelBase");
import serverWideLiveIOStatsWebSocketClient = require("common/serverWideLiveIOStatsWebSocketClient");
import ioStatsGraph = require("models/database/status/ioStatsGraph");

class serverWideIoStats extends viewModelBase {
    private graph: ioStatsGraph;
    
    constructor() {
        super();
        
        this.graph = new ioStatsGraph(
            () => this.activeDatabase().name,
            ["System"],
            false,
            (onUpdate, cutOff) => new serverWideLiveIOStatsWebSocketClient(onUpdate, cutOff));
    }

    compositionComplete() {
        super.compositionComplete();

        const [width, height] = this.getPageHostDimenensions();
        this.graph.init(width, height);
    }
    
    
    deactivate() {
        super.deactivate();

        this.graph.dispose();
    }
}

export = serverWideIoStats;
