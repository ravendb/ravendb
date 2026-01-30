import clusterDashboard = require("viewmodels/resources/clusterDashboard");
import perNodeStatItems = require("models/resources/widgets/perNodeStatItems");
import databaseNotificationsItem = require("models/resources/widgets/databaseNotificationsItem");
import websocketBasedWidget = require("viewmodels/resources/widgets/websocketBasedWidget");
import DatabaseNotificationsWidgetBody = require("components/pages/resources/clusterDashboard/widgets/databaseNotifications/DatabaseNotificationsWidgetBody");
import clusterDashboardWebSocketClient = require("common/clusterDashboardWebSocketClient");
import databasesManager = require("common/shell/databasesManager");
import reactTable = require("@tanstack/react-table");

type DatabaseNotificationsSummaryPayload =
    Raven.Server.Dashboard.Cluster.Notifications.DatabaseNotifications.DatabaseNotificationsSummaryPayload;

interface WidgetConfig {
    ColumnFilters: reactTable.ColumnFiltersState;

    // Alerts and PerformanceHints config are used to get data from server. It's probably not needed but let's keep it for now.
    Alerts: {
        IsEnabled: boolean;
        Reasons: string[];
    };
    PerformanceHints: {
        IsEnabled: boolean;
        Reasons: string[];
    };
}

class databaseNotificationsWidget extends websocketBasedWidget<DatabaseNotificationsSummaryPayload> {
    view = require("views/resources/widgets/databaseNotificationsWidget.html");

    databasesManager = databasesManager.default;

    nodeStats = ko.observableArray<perNodeStatItems<databaseNotificationsItem>>([]);
    columnFilters = ko.observable<reactTable.ColumnFiltersState>([]);

    widgetBodyComponent = ko.observable<ReactInKnockoutOptions<typeof DatabaseNotificationsWidgetBody.default>>(null);

    constructor(controller: clusterDashboard) {
        super(controller);

        for (const node of this.controller.nodes()) {
            const stats = new perNodeStatItems<databaseNotificationsItem>(node.tag());
            this.nodeStats.push(stats);
        }
    }

    getType(): Raven.Server.Dashboard.Cluster.ClusterDashboardNotificationType {
        return "DatabasesNotifications";
    }

    compositionComplete() {
        super.compositionComplete();

        this.enableSyncUpdates();

        for (const ws of this.controller.getConnectedLiveClients()) {
            this.onClientConnected(ws);
        }
    }

    onData(nodeTag: string, data: any) {
        this.scheduleSyncUpdate(() => {
            this.withStats(nodeTag, (x) => (x.items = this.mapItems(nodeTag, data)));
        });
    }

    onClientConnected(ws: clusterDashboardWebSocketClient) {
        super.onClientConnected(ws);

        this.withStats(ws.nodeTag, (x) => x.disconnected(false));
    }

    onClientDisconnected(ws: clusterDashboardWebSocketClient) {
        super.onClientDisconnected(ws);

        this.withStats(ws.nodeTag, (x) => {
            x.items = [];
            x.disconnected(true);
        });
    }

    saveConfiguration(columnFilters: reactTable.ColumnFiltersState): void {
        this.columnFilters(columnFilters);
        this.controller.saveToLocalStorage();
        this.renderReactComponent();
    }

    getConfiguration(): WidgetConfig {
        return {
            ColumnFilters: this.columnFilters(),
            Alerts: {
                IsEnabled: true,
                Reasons: [],
            },
            PerformanceHints: {
                IsEnabled: true,
                Reasons: [],
            },
        };
    }

    restoreConfiguration(config: WidgetConfig) {
        this.columnFilters(config.ColumnFilters);
        this.renderReactComponent();
    }

    protected afterSyncUpdate(updatesCount: number) {
        if (updatesCount === 0) {
            return;
        }

        this.renderReactComponent();
    }

    private withStats(nodeTag: string, action: (stats: perNodeStatItems<databaseNotificationsItem>) => void): void {
        const stats = this.nodeStats().find((x) => x.tag === nodeTag);
        if (stats) {
            action(stats);
        }
    }

    private renderReactComponent() {
        const flatItems = this.createFlatItems();

        this.widgetBodyComponent({
            component: DatabaseNotificationsWidgetBody.default,
            props: {
                flatItems,
                columnFilters: this.columnFilters(),
                setColumnFilters: (x) => this.saveConfiguration(x),
            },
        });
    }

    private createFlatItems(): databaseNotificationsItem[] {
        const flatItems: databaseNotificationsItem[] = ko.unwrap(this.nodeStats()).flatMap((x) => x?.items ?? []);

        const nodesPerDatabase = new Map<string, string[]>();

        flatItems.forEach((item) => {
            const nodes = nodesPerDatabase.get(item.database) || [];
            nodes.push(item.nodeTag);
            nodesPerDatabase.set(item.database, nodes);
        });

        nodesPerDatabase.forEach((nodesWithData, dbName) => {
            const db = this.databasesManager.getDatabaseByName(dbName);
            if (db && db.nodes().length) {
                const allDbNodes = db.nodes();
                for (const dbNode of allDbNodes) {
                    // we want to check if we are not out of sync
                    // as we get data from 2 different endpoints
                    if (!_.includes(nodesWithData, dbNode.tag)) {
                        flatItems.push(this.createNoDataItem(dbNode.tag, dbName));
                    }
                }
            }
        });

        return flatItems;
    }

    private createNoDataItem(nodeTag: string, databaseName: string): databaseNotificationsItem {
        return databaseNotificationsItem.noData(nodeTag, databaseName);
    }

    private mapItems(nodeTag: string, data: DatabaseNotificationsSummaryPayload): databaseNotificationsItem[] {
        return data.NotificationsSummary.map((x) => new databaseNotificationsItem(nodeTag, x));
    }
}

export = databaseNotificationsWidget;
