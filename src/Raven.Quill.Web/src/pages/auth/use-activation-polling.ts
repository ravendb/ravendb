import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { api } from "@/api/api";

const POLL_INTERVAL_MS = 3_000;
const ACTIVATION_TIMEOUT_MS = 120_000;

// Polls appliance bootstrap status until it reports Ready. Activation runs
// automatically at startup, so this just watches the phase and gives up after a
// timeout (with a retry) rather than spinning forever if the appliance never comes up.
export function useActivationPolling() {
    // Pin the start time so the timeout is measured from a fixed point instead of
    // resetting on every successful poll.
    const [startedAt, setStartedAt] = useState(() => Date.now());

    const statusQuery = useQuery({
        ...api.queries.bootstrap.status(),
        // Drive the cadence from refetchInterval alone: with retries off every attempt
        // settles quickly, so a transient boot-time error just becomes the next poll
        // and the timeout math stays simple.
        retry: false,
        refetchInterval: (query) => {
            if (query.state.data?.state === "Ready") {
                return false;
            }

            if (hasReachedTimeout(startedAt, query.state.dataUpdatedAt, query.state.errorUpdatedAt)) {
                return false;
            }

            return POLL_INTERVAL_MS;
        },
    });

    const phase = statusQuery.data?.state;
    const isReady = phase === "Ready";
    const timedOut = !isReady && hasReachedTimeout(startedAt, statusQuery.dataUpdatedAt, statusQuery.errorUpdatedAt);

    function retry() {
        setStartedAt(Date.now());
        void statusQuery.refetch();
    }

    return {
        phase,
        isReady,
        timedOut,
        retry,
    };
}

function hasReachedTimeout(startedAt: number, dataUpdatedAt: number, errorUpdatedAt: number) {
    const lastCheckedAt = Math.max(dataUpdatedAt, errorUpdatedAt);
    return lastCheckedAt - startedAt >= ACTIVATION_TIMEOUT_MS;
}
