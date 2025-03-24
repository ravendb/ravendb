import changesContext from "common/changesContext";
import { useEffect, useState } from "react";
import serverNotificationCenterClient from "common/serverNotificationCenterClient";
import databaseNotificationCenterClient from "common/databaseNotificationCenterClient";
import changesApi from "common/changesApi";
import { createContext, useContext } from "react";
import * as React from "react";
import { ChangesProps } from "hooks/types";

const context = createContext<ChangesProps>(null);

export function ChangesProvider(props: { changes: ChangesProps; children: React.JSX.Element }) {
    return <context.Provider value={props.changes}>{props.children}</context.Provider>;
}

export function useChanges(): ChangesProps {
    const ctx = useContext(context);

    const [serverNotifications, setServerNotifications] = useState<serverNotificationCenterClient>(
        changesContext.default.serverNotifications
    );
    const [databaseNotifications, setDatabaseNotifications] = useState<databaseNotificationCenterClient>(
        changesContext.default.databaseNotifications
    );
    const [databaseChangesApi, setDatabaseChangesApi] = useState<changesApi>(changesContext.default.databaseChangesApi);

    useEffect(() => {
        if (ctx) {
            return;
        }

        const sub = changesContext.default.serverNotifications.subscribe(setServerNotifications);

        return () => {
            sub.dispose();
        };
    }, [ctx]);

    useEffect(() => {
        if (ctx) {
            return;
        }

        const sub = changesContext.default.databaseNotifications.subscribe(setDatabaseNotifications);
        return () => {
            sub.dispose();
        };
    }, [ctx]);

    useEffect(() => {
        if (ctx) {
            return;
        }

        const sub = changesContext.default.databaseChangesApi.subscribe(setDatabaseChangesApi);

        return () => {
            sub.dispose();
        };
    }, [ctx]);

    if (ctx) {
        return ctx;
    }

    return {
        serverNotifications,
        databaseNotifications,
        databaseChangesApi,
    };
}
