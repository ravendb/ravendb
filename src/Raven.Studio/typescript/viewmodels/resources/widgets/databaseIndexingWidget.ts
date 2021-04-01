import textColumn = require("widgets/virtualGrid/columns/textColumn");
import indexingSpeedItem = require("models/resources/widgets/indexingSpeedItem");
import clusterDashboard = require("viewmodels/resources/clusterDashboard");
import nodeTagColumn = require("widgets/virtualGrid/columns/nodeTagColumn");
import abstractTableWidget = require("viewmodels/resources/widgets/abstractTableWidget");
import virtualColumn = require("widgets/virtualGrid/columns/virtualColumn");
import appUrl = require("common/appUrl");
import perNodeStatItems = require("models/resources/widgets/perNodeStatItems");

class databaseIndexingWidget extends abstractTableWidget<Raven.Server.Dashboard.Cluster.Notifications.DatabaseIndexingSpeedPayload, perNodeStatItems<indexingSpeedItem>, indexingSpeedItem> {
    
    getType(): Raven.Server.Dashboard.Cluster.ClusterDashboardNotificationType {
        return "DatabaseIndexing";
    }
    
    noDatabases = ko.observable<boolean>(true);

    constructor(controller: clusterDashboard) {
        super(controller);

        for (const node of this.controller.nodes()) {
            const stats = new perNodeStatItems<indexingSpeedItem>(node.tag());
            this.nodeStats.push(stats);
        }
    }

    protected mapItems(nodeTag: string, data: Raven.Server.Dashboard.Cluster.Notifications.DatabaseIndexingSpeedPayload): indexingSpeedItem[] {
        return data.Items.map(x => new indexingSpeedItem(nodeTag, x));
    }

    onData(nodeTag: string, data: Raven.Server.Dashboard.Cluster.Notifications.DatabaseIndexingSpeedPayload) {
        super.onData(nodeTag, data);
        
        //TODO: what if all items are not relevant on node?
        //TODO: check offline items! 
        this.noDatabases(!data.Items);
    }

    protected customizeGrid() {
        this.gridController().customRowClassProvider(item => item.even ? ["even"] : []);
    }

    protected prepareColumns(containerWidth: number, results: pagedResult<indexingSpeedItem>): virtualColumn[] {
        const grid = this.gridController();
        return [
            new textColumn<indexingSpeedItem>(grid, x => x.hideDatabaseName ? "" : x.database, "Database", "35%"),
            new nodeTagColumn<indexingSpeedItem>(grid, item => this.prepareUrl(item)),
            new textColumn<indexingSpeedItem>(grid, x => x.indexedPerSecond, "Indexed/s", "15%"),
            new textColumn<indexingSpeedItem>(grid, x => x.mappedPerSecond, "Mapped/s", "15%"),
            new textColumn<indexingSpeedItem>(grid, x => x.reducedPerSecond, "Reduced/s", "15%")
        ];
    }

    protected prepareGridData(): JQueryPromise<pagedResult<indexingSpeedItem>> {
        let items: indexingSpeedItem[] = [];
        
        this.nodeStats().forEach(nodeStat => {
            items.push(...nodeStat.items);
        });
        
        //TODO: alpha numeric sort!
        items = _.sortBy(items, ["database", "nodeTag"]);
        
        // leave only first database name in group - we don't want to repeat db name
        let currentDbName = "";
        let even = true;
        
        for (let i = 0; i < items.length; i++) {
            const item = items[i];
            if (item.database === currentDbName) {
                item.hideDatabaseName = true;
            } else {
                currentDbName = item.database;
                even = !even;
            }
            item.even = even;
        }
        
        return $.when({
            totalResultCount: items.length,
            items
        });
    }

    protected prepareUrl(item: indexingSpeedItem): { url: string; openInNewTab: boolean } {
        const database = item.database;
        const nodeTag = item.nodeTag;
        const currentNodeTag = this.clusterManager.localNodeTag();
        const targetNode = this.clusterManager.getClusterNodeByTag(nodeTag)

        const link = appUrl.forIndexPerformance(database);
        if (currentNodeTag === nodeTag) {
            return {
                url: link,
                openInNewTab: false
            };
        } else {
            return {
                url: appUrl.toExternalUrl(targetNode.serverUrl(), link),
                openInNewTab: true
            }
        }
    }
}


export = databaseIndexingWidget;
