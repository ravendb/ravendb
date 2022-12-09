import { EventsCollectorProps } from "hooks/types";

interface ReportedEvent {
    category: string;
    action: string;
    label: string;
}

export default class MockEventsCollector implements EventsCollectorProps {
    private readonly _events: ReportedEvent[] = [];

    reportEvent(category: string, action: string, label: string = null) {
        this._events.push({
            category,
            action,
            label,
        });
    }

    get reportedEvents(): ReportedEvent[] {
        return this._events;
    }

    get mock(): EventsCollectorProps {
        return this;
    }
}
