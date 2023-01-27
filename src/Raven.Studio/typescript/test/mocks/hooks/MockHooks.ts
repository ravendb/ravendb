import MockChangesHook from "test/mocks/hooks/MockChangesHook";
import MockEventsCollector from "test/mocks/hooks/MockEventsCollector";
import { MockClusterTopologyManager } from "test/mocks/hooks/MockClusterTopologyManager";

class MockHooksContainer {
    useChanges = new MockChangesHook();
    useEventsCollector = new MockEventsCollector();
    useClusterTopologyManager = new MockClusterTopologyManager();
}

export const mockHooks = new MockHooksContainer();
