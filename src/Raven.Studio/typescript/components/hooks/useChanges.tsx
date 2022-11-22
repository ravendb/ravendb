import changesContext from "common/changesContext";
import { useEffect, useState } from "react";
import serverNotificationCenterClient from "common/serverNotificationCenterClient";
import databaseNotificationCenterClient from "common/databaseNotificationCenterClient";
import changesApi from "common/changesApi";

export interface ChangesProps {
    serverNotifications: serverNotificationCenterClient;
    databaseNotifications: databaseNotificationCenterClient;
    databaseChangesApi: changesApi;
}

export function useChanges(): ChangesProps {
    const [serverNotifications, setServerNotifications] = useState<serverNotificationCenterClient>(
        changesContext.default.serverNotifications
    );
    const [databaseNotifications, setDatabaseNotifications] = useState<databaseNotificationCenterClient>(
        changesContext.default.databaseNotifications
    );
    const [databaseChangesApi, setDatabaseChangesApi] = useState<changesApi>(changesContext.default.databaseChangesApi);

    useEffect(() => {
        const sub = changesContext.default.serverNotifications.subscribe(setServerNotifications);

        return () => {
            sub.dispose();
        };
    }, []);

    useEffect(() => {
        const sub = changesContext.default.databaseNotifications.subscribe(setDatabaseNotifications);
        return () => {
            sub.dispose();
        };
    }, []);

    useEffect(() => {
        const sub = changesContext.default.databaseChangesApi.subscribe(setDatabaseChangesApi);

        return () => {
            sub.dispose();
        };
    }, []);

    return {
        serverNotifications,
        databaseNotifications,
        databaseChangesApi,
    };
}
