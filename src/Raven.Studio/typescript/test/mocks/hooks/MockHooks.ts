import MockChangesHook from "test/mocks/hooks/MockChangesHook";
import MockEventsCollector from "test/mocks/hooks/MockEventsCollector";

class MockHooksContainer {
    useChanges = new MockChangesHook();
    useEventsCollector = new MockEventsCollector();
}

export const mockHooks = new MockHooksContainer();
