import { ServicesContextDto } from "components/hooks/useServices";
import MockIndexesService from "./MockIndexesService";
import MockDatabasesService from "./MockDatabasesService";
import MockTasksService from "./MockTasksService";
import MockManageServerService from "./MockManageServerService";
import MockLicenseService from "./MockLicenseService";
import MockResourcesService from "test/mocks/services/MockResourcesService";
import MockAiAgentService from "./MockAiAgentService";
import MockAiAssistantService from "./MockAiAssistantService";

class MockServicesContainer {
    indexesService = new MockIndexesService();
    databasesService = new MockDatabasesService();
    tasksService = new MockTasksService();
    manageServerService = new MockManageServerService();
    licenseService = new MockLicenseService();
    resourcesService = new MockResourcesService();
    aiAgentService = new MockAiAgentService();
    aiAssistantService = new MockAiAssistantService();

    get context(): ServicesContextDto {
        return {
            indexesService: this.indexesService.mock,
            databasesService: this.databasesService.mock,
            tasksService: this.tasksService.mock,
            manageServerService: this.manageServerService.mock,
            licenseService: this.licenseService.mock,
            resourcesService: this.resourcesService.mock,
            aiAgentService: this.aiAgentService.mock,
            aiAssistantService: this.aiAssistantService.mock,
        };
    }
}

export const mockServices = new MockServicesContainer();
