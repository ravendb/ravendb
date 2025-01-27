import {
    abstractNotificationCenterClientInterface,
    ChangesProps,
    databaseNotificationCenterClientInterface,
    serverNotificationCenterClientInterface,
} from "hooks/types";
import changeSubscription from "common/changeSubscription";

class MockAbstractNotification implements abstractNotificationCenterClientInterface {
    static readonly _noOpSubscription = new changeSubscription(() => {
        // empty
    });

    watchAllAlerts(
        // eslint-disable-next-line @typescript-eslint/no-unused-vars
        onChange: (e: Raven.Server.NotificationCenter.Notifications.AlertRaised) => void
    ): changeSubscription {
        return MockAbstractNotification._noOpSubscription;
    }
}

class MockDatabaseNotifications extends MockAbstractNotification implements databaseNotificationCenterClientInterface {
    static readonly _noOpSubscription = new changeSubscription(() => {
        // empty
    });

    watchAllDatabaseStatsChanged(
        // eslint-disable-next-line @typescript-eslint/no-unused-vars
        onChange: (e: Raven.Server.NotificationCenter.Notifications.DatabaseStatsChanged) => void
    ): changeSubscription {
        return MockDatabaseNotifications._noOpSubscription;
    }
}

class MockServerNotifications extends MockAbstractNotification implements serverNotificationCenterClientInterface {
    static readonly _noOpSubscription = new changeSubscription(() => {
        // empty
    });

    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    watchReconnect(onChange: () => void): changeSubscription {
        return MockServerNotifications._noOpSubscription;
    }

    watchAllDatabaseChanges(
        // eslint-disable-next-line @typescript-eslint/no-unused-vars
        onChange: (e: Raven.Server.NotificationCenter.Notifications.Server.DatabaseChanged) => void
    ): changeSubscription {
        return MockServerNotifications._noOpSubscription;
    }

    watchClusterTopologyChanges(
        // eslint-disable-next-line @typescript-eslint/no-unused-vars
        onChange: (e: Raven.Server.NotificationCenter.Notifications.Server.ClusterTopologyChanged) => void
    ): changeSubscription {
        return MockServerNotifications._noOpSubscription;
    }
}

export default class MockChangesHook {
    private readonly _mock: ChangesProps = {
        databaseNotifications: new MockDatabaseNotifications(),
        databaseChangesApi: null,
        serverNotifications: new MockServerNotifications(),
    };

    get mock(): ChangesProps {
        return this._mock;
    }
}
