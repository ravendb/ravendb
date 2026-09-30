import { useEffect } from "react";
import { announceHostError, announceHostExpired, announceHostReady } from "@/host-channel";
import type { ChatErrorKind } from "@/transport";

/** Relays a live embed's lifecycle to the page that frames it. Effects rather than derived state, because
 *  the output is a message to another document. */
export function useHostChannel(errorKind: ChatErrorKind | null, errorMessage: string | null): void {
    useEffect(() => {
        announceHostReady();
    }, []);

    // `expired` and `limit` are terminal, so they can only fire once; `failed` is worth re-announcing
    // because the host may want to count retries.
    useEffect(() => {
        if (errorKind === null) return;
        if (errorKind === "failed") announceHostError(errorMessage ?? "chat failed");
        else announceHostExpired(errorKind);
    }, [errorKind, errorMessage]);
}
