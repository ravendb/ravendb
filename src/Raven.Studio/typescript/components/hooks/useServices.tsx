import IndexesService from "../services/IndexesService";
import { createContext, useContext } from "react";
import DatabasesService from "../services/DatabasesService";
import * as React from "react";
import TasksService from "../services/TasksService";
import ManageServerService from "components/services/ManageServerService";

export interface ServicesContextDto {
    indexesService: IndexesService;
    databasesService: DatabasesService;
    tasksService: TasksService;
    manageServerService: ManageServerService;
}

export let services = {
    indexesService: new IndexesService(),
    databasesService: new DatabasesService(),
    tasksService: new TasksService(),
    manageServerService: new ManageServerService(),
};

export function configureMockServices(overloads: typeof services) {
    services = overloads;
}

const servicesContext = createContext<ServicesContextDto>(services);

export function ServiceProvider(props: { services: ServicesContextDto; children: JSX.Element }) {
    return <servicesContext.Provider value={props.services}>{props.children}</servicesContext.Provider>;
}

export const useServices = () => useContext(servicesContext);
