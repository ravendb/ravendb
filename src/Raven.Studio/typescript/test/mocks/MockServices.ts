import { ServicesContextDto } from "../../components/hooks/useServices";
import MockIndexesService from "./MockIndexesService";
import MockDatabasesService from "./MockDatabasesService";

class MockServicesContainer {
    indexesService = new MockIndexesService();
    databasesService = new MockDatabasesService();

    get context(): ServicesContextDto {
        return {
            indexesService: this.indexesService.mock,
            databasesService: this.databasesService.mock,
        };
    }
}

export const mockServices = new MockServicesContainer();
