import { createStoreConfiguration } from "components/store";

const JEST_WORKER_WARN_AFTER_MS = 1000;

export function createTestStore() {
    return createStoreConfiguration({ devChecksWarnAfterMs: JEST_WORKER_WARN_AFTER_MS });
}
