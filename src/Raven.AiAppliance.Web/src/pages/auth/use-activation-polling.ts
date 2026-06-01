import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { api } from "@/api/api";
import type { BootstrapPhase } from "@/api/generated/server-api";

const ACTIVATION_POLL_INTERVAL_MS = 5_000;
const ACTIVATION_TIMEOUT_MS = 120_000;

export function useActivationPolling() {
    const [activationStartedAt, setActivationStartedAt] = useState<number | null>(null);
    const statusQuery = useQuery({
        ...api.queries.bootstrap.status(),
        refetchInterval: (query) => {
            if (!isActivationPending(query.state.data?.state)) {
                return false;
            }

            const startedAt = activationStartedAt ?? query.state.dataUpdatedAt;
            const lastStatusCheckAt = Math.max(query.state.dataUpdatedAt, query.state.errorUpdatedAt);

            return lastStatusCheckAt - startedAt >= ACTIVATION_TIMEOUT_MS ? false : ACTIVATION_POLL_INTERVAL_MS;
        },
    });
    const isPendingStatus = isActivationPending(statusQuery.data?.state);
    const effectiveStartedAt = isPendingStatus ? (activationStartedAt ?? statusQuery.dataUpdatedAt) : null;
    const lastStatusCheckAt = Math.max(statusQuery.dataUpdatedAt, statusQuery.errorUpdatedAt);
    const timedOut =
        effectiveStartedAt !== null &&
        isPendingStatus &&
        lastStatusCheckAt - effectiveStartedAt >= ACTIVATION_TIMEOUT_MS;

    function startActivationPolling() {
        setActivationStartedAt(Date.now());
    }

    function retryActivationPolling() {
        startActivationPolling();
        void statusQuery.refetch();
    }

    return {
        isActivationWaiting: isPendingStatus,
        retryActivationPolling,
        startActivationPolling,
        timedOut,
    };
}

function isActivationPending(state?: BootstrapPhase) {
    return state === "Redeeming" || state === "Restarting";
}
