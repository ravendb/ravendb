import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { api } from "@/api/api";
import { AI_LICENSE_UNAVAILABLE_MESSAGE } from "@/api/custom-services/assistant-service";
import type { AiHelperStatus } from "@/api/generated/server-api";
import { invalidateConsentBlockedSuggestions } from "@/lib/query-invalidation";

export const AI_OUT_OF_TOKENS_MESSAGE = "The AI service has used up its quota for now. Please try again later.";
export const AI_SERVICE_UNAVAILABLE_MESSAGE = "The AI service is unavailable right now. Please try again later.";
export const AI_CHECK_FAILED_MESSAGE = "Could not check whether the AI service is available.";

export function useAiConsentQuery({ enabled = true }: { enabled?: boolean } = {}) {
    return useQuery({ ...api.queries.assistant.consent(), enabled });
}

export type AiConsentState = {
    isPending: boolean;
    isGranted: boolean;
    isConsentRequired: boolean;
    /** The AI service answered and cannot be used. Unlike "not granted", excludes the wait for that answer. */
    isBlocked: boolean;
    /** Why the AI service is out of reach - accepting cannot help. Undefined while it is reachable. */
    unavailableReason: string | undefined;
    /** The obstacle can lift on its own, e.g. a quota window or a service blip. */
    isRetryable: boolean;
    recheck: () => void;
};

export function useAiConsent(): AiConsentState {
    const query = useAiConsentQuery();
    const status = query.data?.status;
    const unavailableReason = describeAiUnavailability(status, query.isError);

    return {
        isPending: query.isPending,
        isGranted: status === "Success",
        isConsentRequired: status === "ConsentRequired",
        isBlocked: !query.isPending && status !== "Success",
        unavailableReason,
        // A license answer is the one the AI service will keep repeating.
        isRetryable: unavailableReason !== undefined && status !== "InvalidCredentials",
        recheck: () => void query.refetch(),
    };
}

/**
 * Why an AI-backed choice cannot be carried out yet, for the wizards that disable their "Next" over it.
 * `manualHint` names the way out in the step's own words.
 */
export function describeAiConsentBlock(consent: AiConsentState, manualHint: string): string | undefined {
    if (consent.isGranted) {
        return undefined;
    }

    if (consent.unavailableReason) {
        return `${consent.unavailableReason} ${manualHint}`;
    }

    if (consent.isPending) {
        return "Checking whether the AI service is available…";
    }

    return `Accept the RavenDB AI Assistant Terms of Use to continue with AI. ${manualHint}`;
}

/** Invalidates the suggestions the missing consent already turned away, so they reload without a page refresh. */
export function useGrantAiConsent({ onGranted }: { onGranted?: () => void } = {}) {
    const queryClient = useQueryClient();

    return useMutation({
        mutationFn: async () => {
            const result = await api.services.assistant.giveConsent();

            if (result.status !== "Success") {
                throw new Error(describeConsentFailure(result.status));
            }

            return result;
        },
        onSuccess: (result) => {
            queryClient.setQueryData(api.queries.assistant.consent().queryKey, result);
            void invalidateConsentBlockedSuggestions(queryClient);
            onGranted?.();
        },
    });
}

/** A cached answer outranks a failed refetch, so the failure message is only used when there is no status. */
function describeAiUnavailability(status: AiHelperStatus | undefined, hasCheckFailed: boolean): string | undefined {
    switch (status) {
        case "Success":
        case "ConsentRequired":
            return undefined;
        case "InvalidCredentials":
            return AI_LICENSE_UNAVAILABLE_MESSAGE;
        case "OutOfTokens":
            return AI_OUT_OF_TOKENS_MESSAGE;
        case undefined:
            return hasCheckFailed ? AI_CHECK_FAILED_MESSAGE : undefined;
        default:
            return AI_SERVICE_UNAVAILABLE_MESSAGE;
    }
}

function describeConsentFailure(status: AiHelperStatus) {
    return status === "ConsentRequired"
        ? "The AI service has not registered the consent yet. Please try again in a moment."
        : (describeAiUnavailability(status, false) ?? AI_SERVICE_UNAVAILABLE_MESSAGE);
}
