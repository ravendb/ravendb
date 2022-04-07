import IndexesService from "../services/IndexesService";
import { createContext, useContext } from "react";
import DatabasesService from "../services/DatabasesService";

export interface ServicesContextDto {
    indexesService: IndexesService;
    databasesService: DatabasesService;
}

const servicesContext = createContext<ServicesContextDto>({
    indexesService: new IndexesService(),
    databasesService: new DatabasesService()
});

export const useServices = () => useContext(servicesContext);

