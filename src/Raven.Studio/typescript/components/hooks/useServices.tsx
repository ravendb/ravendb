import IndexesService from "../services/IndexesService";
import { createContext, useContext } from "react";
import DatabasesService from "../services/DatabasesService";
import * as React from "react";
import TasksService from "../services/TasksService";

export interface ServicesContextDto {
    indexesService: IndexesService;
    databasesService: DatabasesService;
    tasksService: TasksService;
}

const servicesContext = createContext<ServicesContextDto>({
    indexesService: new IndexesService(),
    databasesService: new DatabasesService(),
    tasksService: new TasksService(),
});

export function ServiceProvider(props: { services: ServicesContextDto; children: JSX.Element }) {
    return <servicesContext.Provider value={props.services}>{props.children}</servicesContext.Provider>;
}

export const useServices = () => useContext(servicesContext);
