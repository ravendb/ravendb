import viewModelBase = require("viewmodels/viewModelBase");
import serverWideLiveIOStatsWebSocketClient = require("common/serverWideLiveIOStatsWebSocketClient");
import ioStatsGraph = require("models/database/status/ioStatsGraph");

class serverWideIoStats extends viewModelBase {

    view = require("views/manage/serverWideIoStats.html");
    graphView = require("views/partial/ioStatsGraph.html");
    
    private graph: ioStatsGraph;
    
    constructor() {
        super();
        
        this.graph = new ioStatsGraph(
            () => "Server",
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
