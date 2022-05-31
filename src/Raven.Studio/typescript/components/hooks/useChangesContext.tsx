import changesContext from "common/changesContext";
import { createContext, useContext } from "react";
import serverNotificationCenterClient from "common/serverNotificationCenterClient";
import databaseNotificationCenterClient from "common/databaseNotificationCenterClient";
import * as React from "react";

export interface ChangesContextProps {
    serverNotifications: serverNotificationCenterClient;
    databaseNotifications: () => databaseNotificationCenterClient;
}

const localChangesContext = createContext<ChangesContextProps>({
    serverNotifications: changesContext.default.serverNotifications(),
    databaseNotifications: changesContext.default.databaseNotifications,
});

export function ChangesContextProvider(props: { context: ChangesContextProps; children: JSX.Element }) {
    return <localChangesContext.Provider value={props.context}>{props.children}</localChangesContext.Provider>;
}

export const useChangesContext = () => useContext(localChangesContext);
