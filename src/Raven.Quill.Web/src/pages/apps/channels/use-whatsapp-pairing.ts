import { useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { api } from "@/api/api";

const POLL_INTERVAL_MS = 3_000;
// pairing sessions go stale after WhatsApp's QR budget; force an explicit new code
const PAIRING_TIMEOUT_MS = 300_000;

// Polls the pairing status while the bridge is issuing QR codes. The server always
// returns the current (rotating) payload, so each poll simply re-renders the latest
// QR; Connected/LoggedOut/Disconnected are rest states that stop the polling.
export function useWhatsAppPairing(slug: string, channelId: string) {
    // Pin the start time so the timeout is measured from a fixed point instead of
    // resetting on every successful poll.
    const [startedAt, setStartedAt] = useState(() => Date.now());
    const queryClient = useQueryClient();

    const pairingQuery = useQuery({
        ...api.queries.whatsapp.pairing(slug, channelId),
        // With retries off a poll error (channel deleted, bridge down) settles
        // immediately into the error state instead of hammering the bridge.
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

    // No phone number restarts the QR flow; a number switches to a pairing code.
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
