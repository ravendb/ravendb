import {
    ChangesProps,
    databaseNotificationCenterClientInterface,
    serverNotificationCenterClientInterface,
} from "hooks/types";
import changeSubscription from "common/changeSubscription";

class MockDatabaseNotifications implements databaseNotificationCenterClientInterface {
    private static readonly _noOpSubscription = new changeSubscription(() => {
        // empty
    });

    watchAllDatabaseStatsChanged(
        // eslint-disable-next-line @typescript-eslint/no-unused-vars
        onChange: (e: Raven.Server.NotificationCenter.Notifications.DatabaseStatsChanged) => void
    ): changeSubscription {
        return MockDatabaseNotifications._noOpSubscription;
    }
}

class MockServerNotifications implements serverNotificationCenterClientInterface {
    private static readonly _noOpSubscription = new changeSubscription(() => {
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
