import changesContext from "common/changesContext";
import { createContext, useContext, useEffect, useState } from "react";
import serverNotificationCenterClient from "common/serverNotificationCenterClient";
import databaseNotificationCenterClient from "common/databaseNotificationCenterClient";
import * as React from "react";
import changesApi from "common/changesApi";

export interface ChangesProps {
    serverNotifications: serverNotificationCenterClient;
    databaseNotifications: databaseNotificationCenterClient;
    databaseChangesApi: changesApi;
}

export function useChanges(): ChangesProps {
    const [serverNotifications, setServerNotifications] = useState<serverNotificationCenterClient>();
    const [databaseNotifications, setDatabaseNotifications] = useState<databaseNotificationCenterClient>();
    const [databaseChangesApi, setDatabaseChangesApi] = useState<changesApi>();

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
