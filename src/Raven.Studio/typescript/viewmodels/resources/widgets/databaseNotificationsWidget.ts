import clusterDashboard = require("viewmodels/resources/clusterDashboard");
import nodeTagColumn = require("widgets/virtualGrid/columns/nodeTagColumn");
import abstractDatabaseAndNodeAwareTableWidget = require("viewmodels/resources/widgets/abstractDatabaseAndNodeAwareTableWidget");
import virtualColumn = require("widgets/virtualGrid/columns/virtualColumn");
import textColumn = require("widgets/virtualGrid/columns/textColumn");
import appUrl = require("common/appUrl");
import perNodeStatItems = require("models/resources/widgets/perNodeStatItems");
import DatabaseUtils = require("components/utils/DatabaseUtils");
import databaseNotificationsItem = require("models/resources/widgets/databaseNotificationsItem");
import actionColumn = require("widgets/virtualGrid/columns/actionColumn");
import DatabaseNotificationsWidgetModals = require("components/pages/resources/clusterDashboard/widgets/DatabaseNotificationsWidgetModals");
import awesomeMultiselect = require("common/awesomeMultiselect");

const { SummaryAlertsModal, SummaryPerformanceHintsModal } = DatabaseNotificationsWidgetModals;

type DatabaseNotificationsSummaryPayload = Raven.Server.Dashboard.Cluster.Notifications.DatabaseNotifications.DatabaseNotificationsSummaryPayload;

interface StatusSummary {
    total: number;
    alerts: number;
    performanceHints: number;
}

class databaseNotificationsSummaryWidget extends abstractDatabaseAndNodeAwareTableWidget<
    DatabaseNotificationsSummaryPayload,
    perNodeStatItems<databaseNotificationsItem>,
    databaseNotificationsItem
> {
    view = require("views/resources/widgets/databaseNotificationsWidget.html");

    statusSummary = ko.observable<StatusSummary>();
    
    alertsModal = ko.observable<ReactInKnockoutOptions<typeof SummaryAlertsModal>>(null);
    filteredNodes = ko.observable<string[]>([]);
    allNodes = ko.observable<string[]>([]);
    
    performanceHintsModal = ko.observable<ReactInKnockoutOptions<typeof SummaryPerformanceHintsModal>>(null);
    allNotifications = ["Alerts", "Performance Hints"];
    filteredNotifications = ko.observable(["Alerts", "Performance Hints"]);

    getType(): Raven.Server.Dashboard.Cluster.ClusterDashboardNotificationType {
        return "DatabasesNotifications";
    }

    constructor(controller: clusterDashboard) {
        super(controller);

        for (const node of this.controller.nodes()) {
            const stats = new perNodeStatItems<databaseNotificationsItem>(node.tag());
            this.nodeStats.push(stats);
        }

        const allNodes = this.controller.nodes().map((node) => node.tag());
        this.allNodes(allNodes);
        this.filteredNodes(allNodes);

        this.filteredNotifications.subscribe(() => {
            this.gridController().reset(true);
        });
        this.filteredNodes.subscribe(() => {
            this.gridController().reset(true);
        });
    }

    attached(view: Element, container: HTMLElement) {
        super.attached(view, container);
        
        awesomeMultiselect.build($("#visibleNodesSelector"), opts => {
            opts.includeSelectAllOption = false;
            opts.nSelectedText = " nodes selected";
            opts.allSelectedText = "All nodes selected";
            opts.buttonClass = "border-radius-xxs btn btn-default";
        });
        awesomeMultiselect.build($("#visibleNotificationsSelector"), opts => {
            opts.includeSelectAllOption = false;
            opts.nSelectedText = " notifications selected";
            opts.allSelectedText = "All notifications selected";
            opts.buttonClass = "border-radius-xxs btn btn-default";
        });
    }

    onData(nodeTag: string, data: DatabaseNotificationsSummaryPayload) {
        super.onData(nodeTag, data);
        this.setStatusSummary(data.NotificationsSummary);
    }
    
    protected prepareGridData(): JQueryPromise<pagedResult<databaseNotificationsItem>> {
        let items: databaseNotificationsItem[] = [];
        
        this.nodeStats().forEach(nodeStat => {
            items.push(...nodeStat.items);
        });

        items = items.filter(item => {
            return this.filteredNodes().includes(item.nodeTag);
        });
        
        const nodesPerDatabase = new Map<string, string[]>();
        
        items.forEach(item => {
            const nodes = nodesPerDatabase.get(item.database) || [];
            nodes.push(item.nodeTag);
            nodesPerDatabase.set(item.database, nodes);
        });
        
        nodesPerDatabase.forEach((nodesWithData, dbName) => {
            const db = this.databaseManager.getDatabaseByName(dbName);
            if (db && db.nodes().length) {
                const allDbNodes = db.nodes();
                for (const dbNode of allDbNodes) {
                    if (!_.includes(nodesWithData, dbNode.tag)) {
                        items.push(this.createNoDataItem(dbNode.tag, dbName));
                    }
                }
            }
        });

        this.sortGridData(items);
        
        items = this.manageItems(items);
        
        this.applyPerDatabaseStripes(items);

        return $.when({
            totalResultCount: items.length,
            items
        });
    }

    private setStatusSummary(items: Raven.Server.Dashboard.Cluster.Notifications.DatabaseNotifications.DatabaseNotificationsSummary[]) {
        const summary: StatusSummary = {
            total: 0,
            alerts: 0,
            performanceHints: 0,
        };

        for (const item of items) {
            summary.total += item.AlertsCount + item.PerformanceHintsCount;
            summary.alerts += item.AlertsCount;
            summary.performanceHints += item.PerformanceHintsCount;
        }

        this.statusSummary(summary);
    }

    protected createNoDataItem(nodeTag: string, databaseName: string): databaseNotificationsItem {
        return databaseNotificationsItem.noData(nodeTag, databaseName);
    }

    protected mapItems(nodeTag: string, data: DatabaseNotificationsSummaryPayload): databaseNotificationsItem[] {
        return data.NotificationsSummary.map((x) => new databaseNotificationsItem(nodeTag, x));
    }

    protected manageItems(items: databaseNotificationsItem[]): databaseNotificationsItem[] {
        if (items.length) {
            let commonItem;
            let prevDbName = "";

            for (let i = 0; i < items.length; i++) {
                const item = items[i];
                const currentDbName = item.database;

                if (currentDbName !== prevDbName) {
                    commonItem = databaseNotificationsItem.commonData(item);
                    items.splice(i++, 0, commonItem);
                    prevDbName = currentDbName;
                }
            }
        }

        return items;
    }

    protected applyPerDatabaseStripes(items: databaseNotificationsItem[]) {
        for (let i = 0; i < items.length; i++) {
            const item = items[i];

            if (item.nodeTag) {
                item.even = false;
                item.hideDatabaseName = true;
            } else {
                item.even = true;
            }
        }
    }

    protected prepareColumns(): virtualColumn[] {
        const grid = this.gridController();
        const columns: virtualColumn[] = [
            new textColumn<databaseNotificationsItem>(
                grid,
                (x) => (x.hideDatabaseName ? "" : DatabaseUtils.default.formatName(x.database)),
                "Database",
                "40%"
            ),
            new nodeTagColumn<databaseNotificationsItem>(grid, (item) => this.prepareUrl(item, "Documents View")),
        ];

        if (this.filteredNotifications().includes("Alerts")) {
            columns.push(
                new actionColumn<databaseNotificationsItem>(
                    grid,
                    (item) => this.showAlertsDetails(item),
                    "Alerts",
                    (item) => item.alertsCount ? `<i class="icon-warning"></i> ${item.alertsCount.toLocaleString()}` : "",
                    "20%",
                    {
                        title: () => "Show alerts",
                        extraClass: () => "badge badge-warning rounded-pill padding-left-xxs padding-right-xxs w-fit-content",
                        buttonStyle: "height: 18px;",
                    }
                ),
            );
        }

        if (this.filteredNotifications().includes("Performance Hints")) {
            columns.push(
                new actionColumn<databaseNotificationsItem>(
                    grid,
                    (item) => this.showPerformanceHintsDetails(item),
                    "Perf. hints",
                    (x) => x.performanceHintsCount ? `<i class="icon-performance"></i> ${x.performanceHintsCount.toLocaleString()}` : "",
                    "20%",
                    {
                        title: () => "Show performance hints",
                        extraClass: () => "badge badge-info rounded-pill padding-left-xxs padding-right-xxs w-fit-content",
                        buttonStyle: "height: 18px;",
                    }
                ),
            );
        }
        return columns;
    }

    private showAlertsDetails(details: databaseNotificationsItem) {
        this.alertsModal({
            component: SummaryAlertsModal,
            props: {
                databaseName: details.database,
                nodeTag: details.nodeTag,
                items: details.alerts,
                count: details.alertsCount,
                onClose: () => this.alertsModal(null),
            },
        });
    }

    private showPerformanceHintsDetails(details: databaseNotificationsItem) {
        this.performanceHintsModal({
            component: SummaryPerformanceHintsModal,
            props: {
                databaseName: details.database,
                nodeTag: details.nodeTag,
                items: details.performanceHints,
                count: details.performanceHintsCount,
                onClose: () => this.performanceHintsModal(null),
            },
        });
    }

    protected generateLocalLink(database: string): string {
        return appUrl.forDocuments(null, database);
    }

    getConfiguration(): Raven.Server.Dashboard.Cluster.Notifications.DatabaseNotifications.DatabaseNotificationsSummaryRequestConfig {
        return {
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
}

export = databaseNotificationsSummaryWidget;
