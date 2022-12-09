import eventsCollector from "common/eventsCollector";
import { EventsCollectorProps } from "hooks/types";

export function useEventsCollector(): EventsCollectorProps {
    return eventsCollector.default;
}
