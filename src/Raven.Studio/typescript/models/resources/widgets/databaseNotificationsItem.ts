class databaseNotificationsItem implements Omit<databaseAndNodeAwareStats, "hideDatabaseName" | "even"> {
    database: string;
    nodeTag: string;
    
    alertsCount: number;
    alerts: Raven.Server.Dashboard.Cluster.Notifications.DatabaseNotifications.NotificationSummaryItem[];

    performanceHintsCount: number;
    performanceHints: Raven.Server.Dashboard.Cluster.Notifications.DatabaseNotifications.NotificationSummaryItem[];

    noData: boolean;

    constructor(nodeTag: string, data: Raven.Server.Dashboard.Cluster.Notifications.DatabaseNotifications.DatabaseNotificationsSummary) {
        this.nodeTag = nodeTag;

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

    static noData(nodeTag: string, database: string): databaseNotificationsItem {
        const item = new databaseNotificationsItem(nodeTag, null);
        item.database = database;
        return item;
    }
}

export = databaseNotificationsItem;
