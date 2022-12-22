import MockChangesHook from "test/mocks/hooks/MockChangesHook";
import MockEventsCollector from "test/mocks/hooks/MockEventsCollector";
import { MockDatabaseManager } from "test/mocks/hooks/MockDatabaseManager";

class MockHooksContainer {
    useChanges = new MockChangesHook();
    useEventsCollector = new MockEventsCollector();
    useDatabaseManager = new MockDatabaseManager();
}

export const mockHooks = new MockHooksContainer();
