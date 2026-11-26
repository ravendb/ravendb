import dbLiveIOStatsWebSocketClient = require("common/dbLiveIOStatsWebSocketClient");
import ioStatsGraph = require("models/database/status/ioStatsGraph");
import shardViewModelBase = require("viewmodels/shardViewModelBase");
import database = require("models/resources/database");
import DatabaseUtils = require("components/utils/DatabaseUtils");

class ioStats extends shardViewModelBase {
    
    view = require("views/database/status/ioStats.html");
    graphView = require("views/partial/ioStatsGraph.html");
    
    private readonly graph: ioStatsGraph;
    
    constructor(db: database, location: databaseLocationSpecifier) {
        super(db, location);

        const detailedDatabaseName = DatabaseUtils.default.formatNameForFile(db.name, location);

        // the storage of a shard is reported under the shard name
        const storageDatabaseName = location?.shardNumber != null ? `${db.name}$${location.shardNumber}` : db.name;

        this.graph = new ioStatsGraph(
            () => `database-${detailedDatabaseName}`,
            ["Documents", "Index", "Configuration", "SharedJournals"],
            true,
            (onUpdate, cutOff) => new dbLiveIOStatsWebSocketClient(this.db, this.location, onUpdate, cutOff),
            {
                environmentFilter: env => DatabaseUtils.default.namesEqualIgnoreCase(env.Database, storageDatabaseName),
                nodeTag: location?.nodeTag
            });
    }

    compositionComplete() {
        super.compositionComplete();

        const [width, height] = this.getPageHostDimenensions();
        this.graph.init(width, height);
    }
    
    deactivate() {
        super.deactivate();

        if (this.graph) {
            this.graph.dispose();
        }
    }
}

export = ioStats;
