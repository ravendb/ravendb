import changesApi from "common/changesApi";
import changeSubscription from "common/changeSubscription";

export interface ChangesProps {
    serverNotifications: serverNotificationCenterClientInterface;
    databaseNotifications: databaseNotificationCenterClientInterface;
    databaseChangesApi: changesApi;
}

export interface EventsCollectorProps {
    reportEvent(category: string, action: string, label?: string): void;
}

export interface abstractNotificationCenterClientInterface {
    watchAllAlerts(
        onChange: (e: Raven.Server.NotificationCenter.Notifications.AlertRaised) => void
    ): changeSubscription;
}

export interface databaseNotificationCenterClientInterface extends abstractNotificationCenterClientInterface {
    watchAllDatabaseStatsChanged(
        onChange: (e: Raven.Server.NotificationCenter.Notifications.DatabaseStatsChanged) => void
    ): changeSubscription;
}

export interface serverNotificationCenterClientInterface extends abstractNotificationCenterClientInterface {
    watchReconnect(onChange: () => void): changeSubscription;

    watchClusterTopologyChanges(
        onChange: (e: Raven.Server.NotificationCenter.Notifications.Server.ClusterTopologyChanged) => void
    ): changeSubscription;

    watchAllDatabaseChanges(
        onChange: (e: Raven.Server.NotificationCenter.Notifications.Server.DatabaseChanged) => void
    ): changeSubscription;
}
