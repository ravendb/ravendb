class MockCollector {
    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    reportEvent(category: string, action: string, label: string = null) {
        // empty
    }
}

export function useEventsCollector() {
    return new MockCollector();
}
