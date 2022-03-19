import IndexesService from "../services/IndexesService";
import { createContext, useContext } from "react";

export interface ServicesContextDto {
    indexesService: IndexesService;
}

const servicesContext = createContext<ServicesContextDto>({
    indexesService: new IndexesService()
});

export const useServices = () => useContext(servicesContext);

