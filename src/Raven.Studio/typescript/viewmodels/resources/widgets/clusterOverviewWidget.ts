import websocketBasedWidget = require("viewmodels/resources/widgets/websocketBasedWidget");
import clusterDashboard = require("viewmodels/resources/clusterDashboard");
import clusterDashboardWebSocketClient = require("common/clusterDashboardWebSocketClient");
import virtualGridController = require("widgets/virtualGrid/virtualGridController");
import columnPreviewPlugin = require("widgets/virtualGrid/columnPreviewPlugin");
import virtualColumn = require("widgets/virtualGrid/columns/virtualColumn");
import textColumn = require("widgets/virtualGrid/columns/textColumn");
import iconsPlusTextColumn = require("widgets/virtualGrid/columns/iconsPlusTextColumn");
import nodeTagColumn = require("widgets/virtualGrid/columns/nodeTagColumn");
import actionColumn = require("widgets/virtualGrid/columns/actionColumn");
import clusterTopologyManager = require("common/shell/clusterTopologyManager");
import generalUtils = require("common/generalUtils");
import appUrl = require("common/appUrl");
import router = require("plugins/router");
import moment = require("moment");

class nodeStatsItem {
    
    nodeTag: string;
    nodeUrl = ko.observable<string>();
    
    nodeType = ko.observable<clusterNodeType>();
    nodeState = ko.observable<Raven.Client.ServerWide.RachisState>();

    startTime = ko.observable<string>();
    formattedUpTime = ko.observable<string>();
    formattedStartTime: KnockoutComputed<string>;

    disconnected = ko.observable<boolean>(true);
    noData: boolean = true;
    
    even: boolean = false;

    constructor(tag: string) {
        this.nodeTag = tag;

        this.formattedStartTime = ko.pureComputed(() => {
            const start = this.startTime();
            return start ? generalUtils.formatUtcDateAsLocal(start) : "";
        });
        
        this.disconnected.subscribe((disconnected) => {
            if (disconnected) {
                this.noData = true;
                this.nodeState(null);
                this.nodeType(null);
                this.nodeUrl(null);
                this.startTime(null);
                this.formattedUpTime(null);
            }
        })
    }

    update(data: Raven.Server.Dashboard.Cluster.Notifications.ClusterOverviewPayload) {
        this.noData = false;
        
        this.nodeType(data.NodeType as clusterNodeType);
        this.nodeState(data.NodeState);
        this.nodeUrl(data.NodeUrl);
        
        this.startTime(data.StartTime);
        this.formattedUpTime(this.getUpTime(data.StartTime));
    }

    private getUpTime(startTime: string) {
        if (!startTime) {
            this.formattedUpTime("a few seconds");
        }
        
        return generalUtils.formatDurationByDate(moment(startTime));
    }
}

class clusterOverviewWidget extends websocketBasedWidget<Raven.Server.Dashboard.Cluster.Notifications.ClusterOverviewPayload> {

    view = require("views/resources/widgets/clusterOverviewWidget.html");
    private clusterManager = clusterTopologyManager.default;
    
    private gridController = ko.observable<virtualGridController<nodeStatsItem>>();
    private columnPreview = new columnPreviewPlugin<nodeStatsItem>();
    
    nodeStats = ko.observableArray<nodeStatsItem>([]);

    constructor(controller: clusterDashboard) {
        super(controller);

        for (const node of this.controller.nodes()) {
            const stats = new nodeStatsItem(node.tag());
            this.nodeStats.push(stats);
        }
    }

    getType(): Raven.Server.Dashboard.Cluster.ClusterDashboardNotificationType {
        return "ClusterOverview";
    }

    compositionComplete() {
        super.compositionComplete();

        const grid = this.gridController();
        grid.headerVisible(true);

        this.gridController().customRowClassProvider(item => item.even ? ["even"] : []);

        grid.init(() => this.prepareGridData(), () => this.prepareColumns());

        this.columnPreview.install(".cluster-overview-grid", ".js-preview",
            (nodeItem: nodeStatsItem, column: virtualColumn, e: JQueryEventObject, onValue: (context: any, valueToCopy?: string) => void) => {
                if (column instanceof textColumn && column.header === "Start time") {
                    onValue(moment.utc(nodeItem.startTime()), nodeItem.startTime());
                }
            });
        
        this.enableSyncUpdates();

        for (let ws of this.controller.getConnectedLiveClients()) {
            this.onClientConnected(ws);
        }

        this.initTooltips();
    }

    private prepareGridData(): JQueryPromise<pagedResult<nodeStatsItem>> {
        let items = this.nodeStats();

        this.applyPerDatabaseStripes(items);
        
        return $.when({
            totalResultCount: items.length,
            items
        });
    }

    protected prepareColumns(): virtualColumn[] {
        const grid = this.gridController();
        
        return [
            new nodeTagColumn<nodeStatsItem>(grid, item => this.prepareUrl(item, "Cluster Dashboard")),

            new iconsPlusTextColumn<nodeStatsItem>(grid, item => item.disconnected() ? "Trying to connect.." : (item.nodeType() ? this.nodeTypeDataForHtml(item.nodeType()) : ""), "Type", "15%"),

            new iconsPlusTextColumn<nodeStatsItem>(grid, item => item.nodeState() ? this.nodeStateDataForHtml(item.nodeState()) : "", "State", "15%"),

            new textColumn<nodeStatsItem>(grid, item => item.formattedUpTime() || "", "Up time", "15%"),

            new textColumn<nodeStatsItem>(grid, item => item.formattedStartTime() || "", "Start time", "20%"),
            
            new actionColumn<nodeStatsItem>(grid, item => router.navigate(item.nodeUrl()), "URL", item => item.nodeUrl() || "" , "20px",
                {
                    title: () => 'Go to URL'
                }),
        ];
    }
    
    private nodeTypeDataForHtml(type: clusterNodeType): iconPlusText[] {
        let iconClass;
        
        switch (type) {
            case "Member": iconClass = "icon-cluster-member";
            break;
            case "Promotable": iconClass = "icon-cluster-promotable";
            break;
            case "Watcher": iconClass= "icon-cluster-watcher";
            break;
            default:
                console.warn("Invalid node type: " + type);
            break;
        }
        return [{
            title: "",
            text: type,
            iconClass: iconClass,
            textClass: ""
        }];
    }

    private nodeStateDataForHtml(state: Raven.Client.ServerWide.RachisState): iconPlusText[] {
        let iconClass;

        switch (state) {
            case "Leader":
                iconClass = "icon-node-leader";
                break;

            case "Passive":
            case "Candidate":
            case "Follower":
            case "LeaderElect":
                iconClass = "";
                break;

            default:
                console.warn("Invalid node state: " + state);
                break;
        }

        return [{
            title: "",
            text: state,
            iconClass: iconClass,
            textClass: ""
        }];
    }

    private prepareUrl(item: nodeStatsItem, targetDescription: string): { url: string; openInNewTab: boolean, noData: boolean, targetDescription?: string } {
        const nodeTag = item.nodeTag;

        if (item.noData) {
            return {
                url: null,
                noData: true,
                openInNewTab: false,
                targetDescription: null
            }
        }

        const currentNodeTag = this.clusterManager.localNodeTag();
        const targetNode = this.clusterManager.getClusterNodeByTag(nodeTag);

        const link = appUrl.forClusterDashboard();

        if (currentNodeTag === nodeTag) {
            return {
                url: link,
                noData: false,
                openInNewTab: false,
                targetDescription: targetDescription
            };
        } else {
            return {
                url: appUrl.toExternalUrl(targetNode.serverUrl(), link),
                noData: false, // todo ???
                openInNewTab: true,
                targetDescription: targetDescription
            }
        }
    }
    
    private initTooltips() {
        $('[data-toggle="tooltip"]', this.container).tooltip();
    }

    onData(nodeTag: string, data: Raven.Server.Dashboard.Cluster.Notifications.ClusterOverviewPayload) {
        this.scheduleSyncUpdate(() =>
            this.withStats(nodeTag, x => x.update(data)));
    }

    onClientConnected(ws: clusterDashboardWebSocketClient) {
        super.onClientConnected(ws);
        
        this.withStats(ws.nodeTag, x => x.disconnected(false)); // ??? todo
    }

    onClientDisconnected(ws: clusterDashboardWebSocketClient) {
        super.onClientDisconnected(ws);
        
        this.withStats(ws.nodeTag, (x) => {
            x.disconnected(true);
        })

        this.gridController().reset(false);
    }

    private withStats(nodeTag: string, action: (stats: nodeStatsItem) => void) {
        const stats = this.nodeStats().find(item => item.nodeTag === nodeTag);
        if (stats) {
            action(stats);
        }
    }

    protected afterSyncUpdate(updatesCount: number) {
        this.gridController().reset(false);
        
        this.initTooltips();
    }

    protected applyPerDatabaseStripes(items: nodeStatsItem[]) {
        let even = true;

        for (let i = 0; i < items.length; i++) {
            const item = items[i];
            even = !even;
            item.even = even;
        }
    }
}

export = clusterOverviewWidget;
