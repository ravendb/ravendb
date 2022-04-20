class MockCollector {
    reportEvent(category: string, action: string, label: string = null) {}
}

export function useEventsCollector() {
    return new MockCollector();
}
