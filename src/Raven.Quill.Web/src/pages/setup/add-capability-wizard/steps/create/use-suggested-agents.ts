import { useLayoutEffect, type ReactNode } from "react";
import { useIsFetching, useQuery } from "@tanstack/react-query";
import { useFormContext, useWatch } from "react-hook-form";
import { useParams } from "react-router";
import { api } from "@/api/api";
import { describeAiConsentBlock, useAiConsent } from "@/components/ai-consent/use-ai-consent";
import { getFetchStartedAt } from "@/lib/query-fetch-start";
import { applySuggestionToForm } from "@/pages/setup/add-capability-wizard/agent-config-form";
import { useCapabilityWizardStore } from "@/pages/setup/add-capability-wizard/capability-wizard-store";
import type { AgentFormData } from "@/pages/setup/add-capability-wizard/capability-wizard-validation";

/**
 * Loads the data-derived agent candidates into the wizard store. The connect step prefetches the
 * same query, so this usually joins a call that is already in flight - and the caller renders
 * progress for however much of it is left.
 */
export function useSuggestedAgents(): {
    isSuggesting: boolean;
    startedAt: number | undefined;
    isConsentRequired: boolean;
    recheck: () => void;
} {
    const { slug = "" } = useParams();
    const { getValues, setValue } = useFormContext<AgentFormData>();
    const suggestions = useCapabilityWizardStore((state) => state.suggestions);
    const setSuggestions = useCapabilityWizardStore((state) => state.setSuggestions);

    const { isGranted, recheck: recheckConsent } = useAiConsent();
    const suggestQuery = api.queries.apps.suggestAgentFromData(slug);
    // Without consent the call can only come back refused; accepting flips this on and the
    // suggestions load without leaving the step.
    const query = useQuery({ ...suggestQuery, enabled: isGranted });
    const suggestedAgents = query.data?.configurations;

    // The candidates arrive after this step is already on screen, so handing them to the store is a
    // genuine sync step. The store mirrors the cached array by reference, which applies each fetch
    // result exactly once and leaves the operator's edits alone when they navigate back and forth.
    // Runs before paint so a suggestion that is already cached never flashes the progress skeleton.
    useLayoutEffect(() => {
        if (!suggestedAgents || suggestedAgents === suggestions) {
            return;
        }

        setSuggestions(suggestedAgents);

        // Seed the review configuration from the first candidate, unless the operator picked another
        // creation mode while the suggestions were still loading.
        if (suggestedAgents.length > 0 && getValues("create.mode") === "ai") {
            applySuggestionToForm(setValue, suggestedAgents[0], 0);
        }
    }, [getValues, setSuggestions, setValue, suggestedAgents, suggestions]);

    // The store keeps the answer for the rest of the wizard, so the step never falls back to the
    // skeleton if the cache entry is evicted while the operator is on a later step. An empty result
    // stays stale and refetches on remount, so the fetch state must count as suggesting too - the
    // footer already blocks "Next" for it, and the body must not declare failure meanwhile.
    return {
        isSuggesting: (query.isFetching || !suggestedAgents) && suggestions.length === 0,
        startedAt: getFetchStartedAt(suggestQuery.queryKey),
        isConsentRequired: query.data?.isConsentRequired === true,
        recheck: () => {
            recheckConsent();
            void query.refetch();
        },
    };
}

/**
 * Why the wizard should hold "Next" back on the create step: advancing without a configuration would
 * only carry an empty one into the review step. Observes the cache instead of the query so the step
 * definitions never start a fetch of their own.
 */
export function useCreateAgentNextBlock(): { isNextDisabled: boolean; nextDisabledReason?: ReactNode } {
    const { slug = "" } = useParams();
    const { control } = useFormContext<AgentFormData>();
    const mode = useWatch({ control, name: "create.mode" });
    const hasSuggestions = useCapabilityWizardStore((state) => state.suggestions.length > 0);
    const isFetching = useIsFetching({ queryKey: api.queries.apps.suggestAgentFromData(slug).queryKey }) > 0;
    const consent = useAiConsent();

    const consentBlock =
        mode === "manual" ? undefined : describeAiConsentBlock(consent, "Or set the agent up manually.");
    if (consentBlock) {
        return { isNextDisabled: true, nextDisabledReason: consentBlock };
    }

    if (mode === "ai" && isFetching && !hasSuggestions) {
        return { isNextDisabled: true, nextDisabledReason: "Waiting for the AI-suggested agents." };
    }

    return { isNextDisabled: false };
}
