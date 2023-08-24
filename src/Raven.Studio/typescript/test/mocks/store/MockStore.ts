import { MockDatabaseManager } from "test/mocks/store/MockDatabaseManager";
import { MockAccessManager } from "test/mocks/store/MockAccessManager";
import { MockClusterManager } from "test/mocks/store/MockClusterManager";
import { MockLicenseManager } from "./MockLicenseManager";

class MockStoreContainer {
    databases = new MockDatabaseManager();
    accessManager = new MockAccessManager();
    cluster = new MockClusterManager();
    license = new MockLicenseManager();
}

export const mockStore = new MockStoreContainer();
