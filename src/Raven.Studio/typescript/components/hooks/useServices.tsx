import IndexesService from "../services/IndexesService";
import { createContext, useContext } from "react";
import DatabasesService from "../services/DatabasesService";
import TasksService from "../services/TasksService";
import ManageServerService from "components/services/ManageServerService";
import LicenseService from "components/services/LicenseService";
import ResourcesService from "components/services/ResourcesService";
import SetupWizardService from "components/services/SetupWizardService";
import AiAgentService from "components/services/AiAgentService";
import AiAssistantService from "components/services/AiAssistantService";

export interface ServicesContextDto {
    indexesService: IndexesService;
    databasesService: DatabasesService;
    tasksService: TasksService;
    manageServerService: ManageServerService;
    licenseService: LicenseService;
    resourcesService: ResourcesService;
    setupWizardService: SetupWizardService;
    aiAgentService: AiAgentService;
    aiAssistantService: AiAssistantService;
}

export let services = {
    indexesService: new IndexesService(),
    databasesService: new DatabasesService(),
    tasksService: new TasksService(),
    manageServerService: new ManageServerService(),
    licenseService: new LicenseService(),
    resourcesService: new ResourcesService(),
    setupWizardService: new SetupWizardService(),
    aiAgentService: new AiAgentService(),
    aiAssistantService: new AiAssistantService(),
};

export function configureMockServices(overloads: typeof services) {
    services = overloads;
}

const servicesContext = createContext<ServicesContextDto>(services);

export function ServiceProvider(props: { services: ServicesContextDto; children: React.JSX.Element }) {
    return <servicesContext.Provider value={props.services}>{props.children}</servicesContext.Provider>;
}

export const useServices = () => useContext(servicesContext);
