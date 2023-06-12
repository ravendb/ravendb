import MockChangesHook from "test/mocks/hooks/MockChangesHook";
import MockEventsCollector from "test/mocks/hooks/MockEventsCollector";
import MockDirtyFlagHook from "./MockDirtyFlagHook";

class MockHooksContainer {
    useChanges = new MockChangesHook();
    useEventsCollector = new MockEventsCollector();
    useDirtyFlag = new MockDirtyFlagHook();
}

export const mockHooks = new MockHooksContainer();
