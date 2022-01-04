import viewModelBase = require("viewmodels/viewModelBase");
import dbLiveIOStatsWebSocketClient = require("common/dbLiveIOStatsWebSocketClient");
import ioStatsGraph = require("models/database/status/ioStatsGraph");

class ioStats extends viewModelBase {
    
    view = require("views/database/status/ioStats.html");
    graphView = require("views/partial/ioStatsGraph.html");
    
    private graph: ioStatsGraph;
    
    constructor() {
        super();
        
        this.graph = new ioStatsGraph(
            () => `database-${this.activeDatabase().name}`,
            ["Documents", "Index", "Configuration"],
            true,
            (onUpdate, cutOff) => new dbLiveIOStatsWebSocketClient(this.activeDatabase(), onUpdate, cutOff));
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

export = ioStats;
