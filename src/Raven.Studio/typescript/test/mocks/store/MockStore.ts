import { MockDatabaseManager } from "test/mocks/store/MockDatabaseManager";

class MockStoreContainer {
    databases = new MockDatabaseManager();
}

export const mockStore = new MockStoreContainer();
