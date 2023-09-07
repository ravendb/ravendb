import { ServicesContextDto } from "components/hooks/useServices";
import MockIndexesService from "./MockIndexesService";
import MockDatabasesService from "./MockDatabasesService";
import MockTasksService from "./MockTasksService";
import MockManageServerService from "./MockManageServerService";
import MockLicenseService from "./MockLicenseService";

class MockServicesContainer {
    indexesService = new MockIndexesService();
    databasesService = new MockDatabasesService();
    tasksService = new MockTasksService();
    manageServerService = new MockManageServerService();
    licenseService = new MockLicenseService();

    get context(): ServicesContextDto {
        return {
            indexesService: this.indexesService.mock,
            databasesService: this.databasesService.mock,
            tasksService: this.tasksService.mock,
            manageServerService: this.manageServerService.mock,
            licenseService: this.licenseService.mock,
        };
    }
}

export const mockServices = new MockServicesContainer();
