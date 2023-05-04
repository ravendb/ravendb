import websocketBasedWidget = require("viewmodels/resources/widgets/websocketBasedWidget");
import clusterDashboard = require("viewmodels/resources/clusterDashboard");
import clusterDashboardWebSocketClient = require("common/clusterDashboardWebSocketClient");
import clusterTopologyManager = require("common/shell/clusterTopologyManager");
import generalUtils = require("common/generalUtils");
import appUrl = require("common/appUrl");
import moment = require("moment");
import assertUnreachable from "components/utils/assertUnreachable";
import IconName from "../../../../typings/server/icons";
import clusterNode from "models/database/cluster/clusterNode";
import OSType = Raven.Client.ServerWide.Operations.OSType;

class nodeStatsItem {
    
    nodeTag: string;
    nodeUrl = ko.observable<string>();
    
    nodeType = ko.observable<clusterNodeType>();
    nodeState = ko.observable<Raven.Client.ServerWide.RachisState>();

    startTime = ko.observable<string>();
    formattedUpTime = ko.observable<string>();
    formattedStartTime: KnockoutComputed<string>;
    
    serverVersion = ko.observable<string>();
    osName = ko.observable<string>();
    osType = ko.observable<OSType>();
    
    osIcon: KnockoutComputed<string>;

    disconnected = ko.observable<boolean>(true);
    noData = ko.observable<boolean>(true);

    constructor(tag: string) {
        this.nodeTag = tag;

        this.initObservables();
    }

    initObservables() {
        this.formattedStartTime = ko.pureComputed(() => {
            const start = this.startTime();
            return start ? generalUtils.formatUtcDateAsLocal(start) : "";
        });
        
        this.disconnected.subscribe((disconnected) => {
            if (disconnected) {
                this.noData(true);
                this.nodeState(null);
                this.nodeType(null);
                this.nodeUrl(null);
                this.startTime(null);
                this.formattedUpTime(null);
                this.serverVersion(null);
                this.osName(null);
                this.osType(null);
            }
        });
        
        this.osIcon = ko.pureComputed(() => clusterNode.osIcon(this.osType()));
    }

    update(data: Raven.Server.Dashboard.Cluster.Notifications.ClusterOverviewPayload) {
        this.noData(false);
        
        this.nodeType(data.NodeType as clusterNodeType);
        this.nodeState(data.NodeState);
        this.nodeUrl(data.NodeUrl);
        
        this.startTime(data.StartTime);
        this.formattedUpTime(this.getUpTime(data.StartTime));
        
        this.serverVersion(data.ServerVersion);
        this.osName(data.OsName);
        this.osType(data.OsType);
    }

    private getUpTime(startTime: string) {
        if (!startTime) {
            this.formattedUpTime("a few seconds");
        }
        
        return generalUtils.formatDurationByDate(moment.utc(startTime));
    }

    iconClass(type: clusterNodeType): IconName {
        if (!type) {
            return "about";
        }
        
        switch (type) {
            case "Member":
                return "cluster-member";
            case "Promotable":
                return "cluster-promotable";
            case "Watcher":
                return "cluster-watcher";
            default:
                assertUnreachable(type);
        }
    }
}

class clusterOverviewWidget extends websocketBasedWidget<Raven.Server.Dashboard.Cluster.Notifications.ClusterOverviewPayload> {

    view = require("views/resources/widgets/clusterOverviewWidget.html");
    private clusterManager = clusterTopologyManager.default;
    
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
        
        this.enableSyncUpdates();

        for (const ws of this.controller.getConnectedLiveClients()) {
            this.onClientConnected(ws);
        }
        this.initTooltips();
    }
    
    private initTooltips() {
        $('[data-toggle="tooltip"]', this.container).tooltip();
    }

    prepareUrl(item: nodeStatsItem): { url: string; openInNewTab: boolean } {
        const nodeTag = item.nodeTag;

        if (item.noData()) {
            return { 
                url: null,
                openInNewTab: false
            };
        }

        const currentNodeTag = this.clusterManager.localNodeTag();
        const targetNode = this.clusterManager.getClusterNodeByTag(nodeTag);

        const link = appUrl.forClusterDashboard();

        if (currentNodeTag === nodeTag) {
            return {
                url: link,
                openInNewTab: false,
            };
        } else {
            return {
                url: appUrl.toExternalUrl(targetNode.serverUrl(), link),
                openInNewTab: true,
            }
        }
    }

    onData(nodeTag: string, data: Raven.Server.Dashboard.Cluster.Notifications.ClusterOverviewPayload) {
        this.scheduleSyncUpdate(() =>
            this.withStats(nodeTag, x => x.update(data)));
    }

    onClientConnected(ws: clusterDashboardWebSocketClient) {
        super.onClientConnected(ws);
        
        this.withStats(ws.nodeTag, x => x.disconnected(false));
    }

    onClientDisconnected(ws: clusterDashboardWebSocketClient) {
        super.onClientDisconnected(ws);

        this.withStats(ws.nodeTag, x => x.disconnected(true));
    }

    private withStats(nodeTag: string, action: (stats: nodeStatsItem) => void) {
        const stats = this.nodeStats().find(item => item.nodeTag === nodeTag);
        if (stats) {
            action(stats);
        }
    }
}

export = clusterOverviewWidget;
