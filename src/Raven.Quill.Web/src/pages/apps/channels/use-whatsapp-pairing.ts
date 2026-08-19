import { useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { api } from "@/api/api";

const POLL_INTERVAL_MS = 3_000;
const PAIRING_TIMEOUT_MS = 300_000;

export function useWhatsAppPairing(slug: string, channelId: string) {
    const [startedAt, setStartedAt] = useState(() => Date.now());
    const queryClient = useQueryClient();

    const pairingQuery = useQuery({
        ...api.queries.whatsapp.pairing(slug, channelId),
        retry: false,
        refetchInterval: (query) => {
            if (query.state.status === "error") {
                return false;
            }

            const state = query.state.data?.state;
            if (state !== undefined && state !== "Starting" && state !== "Pairing") {
                return false;
            }

            if (hasReachedTimeout(startedAt, query.state.dataUpdatedAt, query.state.errorUpdatedAt)) {
                return false;
            }

            return POLL_INTERVAL_MS;
        },
    });

    const restartMutation = useMutation({
        mutationFn: (phoneNumber?: string) =>
            api.services.whatsapp.pairingRestart(slug, channelId, { phoneNumber: phoneNumber ?? null }),
        onSuccess: async (pairing) => {
            setStartedAt(Date.now());
            queryClient.setQueryData(api.queries.whatsapp.pairing(slug, channelId).queryKey, pairing);
            await pairingQuery.refetch();
        },
    });

    const state = pairingQuery.data?.state;
    const isSettled = state !== undefined && state !== "Starting" && state !== "Pairing";
    const hasTimedOut =
        !isSettled &&
        !pairingQuery.isError &&
        hasReachedTimeout(startedAt, pairingQuery.dataUpdatedAt, pairingQuery.errorUpdatedAt);

    return {
        pairing: pairingQuery.data,
        isPending: pairingQuery.isPending,
        isError: pairingQuery.isError,
        hasTimedOut,
        retry: () => {
            setStartedAt(Date.now());
            void pairingQuery.refetch();
        },
        restart: () => restartMutation.mutate(undefined),
        restartWithPhoneNumber: (phoneNumber: string) => restartMutation.mutate(phoneNumber),
        isRestarting: restartMutation.isPending,
        restartError: restartMutation.error,
    };
}

function hasReachedTimeout(startedAt: number, dataUpdatedAt: number, errorUpdatedAt: number) {
    const lastCheckedAt = Math.max(dataUpdatedAt, errorUpdatedAt);
    return lastCheckedAt - startedAt >= PAIRING_TIMEOUT_MS;
}
