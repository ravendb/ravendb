class databaseNotificationsSummaryItem implements databaseAndNodeAwareStats {
    database: string;
    nodeTag: string;
    
    alertsCount: number;
    alerts: Raven.Server.Dashboard.Cluster.Notifications.DatabaseNotifications.NotificationSummaryItem[];

    performanceHintsCount: number;
    performanceHints: Raven.Server.Dashboard.Cluster.Notifications.DatabaseNotifications.NotificationSummaryItem[];

    noData: boolean;

    hideDatabaseName: boolean;
    even = false;

    constructor(nodeTag: string, data: Raven.Server.Dashboard.Cluster.Notifications.DatabaseNotifications.DatabaseNotificationsSummary) {
        this.nodeTag = nodeTag;
        this.hideDatabaseName = false;

        if (data) {
            this.noData = false;
            this.database = data.DatabaseName;
            this.alertsCount = data.AlertsCount;
            this.alerts = data.Alerts;
            this.performanceHintsCount = data.PerformanceHintsCount;
            this.performanceHints = data.PerformanceHints;
        } else {
            this.noData = true;
        }
    }

    static noData(nodeTag: string, database: string): databaseNotificationsSummaryItem {
        const item = new databaseNotificationsSummaryItem(nodeTag, null);
        item.database = database;
        return item;
    }

    static commonData(item: databaseNotificationsSummaryItem) {
        const commonItem = new databaseNotificationsSummaryItem(null, null);

        commonItem.database = item.database;
        commonItem.alertsCount = item.alertsCount;
        commonItem.alerts = item.alerts;
        commonItem.performanceHintsCount = item.performanceHintsCount;
        commonItem.performanceHints = item.performanceHints;

        return commonItem;
    }

    alertsDataForHtml(): iconPlusText[] {
        if (!this.alerts?.length) {
            return [];
        }

        const textValue = this.alertsCount.toLocaleString();
        return [
            {
                title: `${textValue} ${this.alertsCount > 1 ? "alerts" : "alert"}`,
                text: textValue,
                iconClass: "icon-warning",
                textClass: "badge badge-warning rounded-pill",
            }
        ];
    }

    performanceHintsDataForHtml(): iconPlusText[] {
        if (!this.performanceHints?.length) {
            return [];
        }

        const textValue = this.performanceHintsCount.toLocaleString();
        return [
            {
                title: `${textValue} ${this.performanceHintsCount > 1 ? "performance hints" : "performance hint"}`,
                text: textValue,
                iconClass: "icon-info",
                textClass: "badge badge-info rounded-pill",
            },
        ];
    }
}

export = databaseNotificationsSummaryItem;
