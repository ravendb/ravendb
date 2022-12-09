import { mockHooks } from "test/mocks/hooks/MockHooks";
import { EventsCollectorProps } from "hooks/types";

export function useEventsCollector(): EventsCollectorProps {
    return mockHooks.useEventsCollector.mock;
}
