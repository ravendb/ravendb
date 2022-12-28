import MockChangesHook from "test/mocks/hooks/MockChangesHook";
import MockEventsCollector from "test/mocks/hooks/MockEventsCollector";
import { MockDatabaseManager } from "test/mocks/hooks/MockDatabaseManager";
import { MockClusterTopologyManager } from "test/mocks/hooks/MockClusterTopologyManager";

class MockHooksContainer {
    useChanges = new MockChangesHook();
    useEventsCollector = new MockEventsCollector();
    useDatabaseManager = new MockDatabaseManager();
    useClusterTopologyManager = new MockClusterTopologyManager();
}

export const mockHooks = new MockHooksContainer();
