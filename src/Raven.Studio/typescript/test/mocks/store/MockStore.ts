import { MockDatabaseManager } from "test/mocks/store/MockDatabaseManager";
import { MockAccessManager } from "test/mocks/store/MockAccessManager";
import { MockClusterManager } from "test/mocks/store/MockClusterManager";
import { MockLicenseManager } from "./MockLicenseManager";
import { MockCollectionsTracker } from "./MockCollectionsTracker";
import { MockAdminLogs } from "test/mocks/store/MockAdminLogs";
import { MockAiAssistant } from "./MockAIAssistant";

class MockStoreContainer {
    databases = new MockDatabaseManager();
    accessManager = new MockAccessManager();
    cluster = new MockClusterManager();
    license = new MockLicenseManager();
    collectionsTracker = new MockCollectionsTracker();
    adminLogs = new MockAdminLogs();
    aiAssistant = new MockAiAssistant();
}

export const mockStore = new MockStoreContainer();
