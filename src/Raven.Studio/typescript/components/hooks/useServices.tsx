import IndexesService from "../services/IndexesService";
import { createContext, useContext } from "react";
import DatabasesService from "../services/DatabasesService";
import * as React from "react";

export interface ServicesContextDto {
    indexesService: IndexesService;
    databasesService: DatabasesService;
}

const servicesContext = createContext<ServicesContextDto>({
    indexesService: new IndexesService(),
    databasesService: new DatabasesService(),
});

export function ServiceProvider(props: { services: ServicesContextDto; children: JSX.Element }) {
    return <servicesContext.Provider value={props.services}>{props.children}</servicesContext.Provider>;
}

export const useServices = () => useContext(servicesContext);
