import { useFormContext } from "react-hook-form";
import { useParams } from "react-router";
import { useQueryClient } from "@tanstack/react-query";
import { api } from "@/api/api";
import type { AgentFormData } from "@/pages/setup/add-capability-wizard/capability-wizard-validation";
import { applySuggestionToForm } from "@/pages/setup/add-capability-wizard/agent-config-form";
import { useCapabilityWizardStore } from "@/pages/setup/add-capability-wizard/capability-wizard-store";

export function useConnectProviderStep() {
    const { slug = "" } = useParams();
    const { setValue } = useFormContext<AgentFormData>();
    const queryClient = useQueryClient();
    const setSuggestions = useCapabilityWizardStore((state) => state.setSuggestions);
    const suggestions = useCapabilityWizardStore((state) => state.suggestions);

    return async () => {
        // Already fetched (e.g. the operator went Back then Next again) — keep their edits
        // instead of clobbering the form with a fresh suggestion.
        if (suggestions.length > 0) {
            return;
        }

        // ConnectProviderStep prefetches this query on step entry; fetchQuery joins the
        // in-flight request (or returns the cached result) instead of starting a new call.
        const configurations = await queryClient.fetchQuery(api.queries.apps.suggestAgentFromData(slug));
        setSuggestions(configurations);

        // No suggestions is fine — the create step offers prompt and manual modes.
        if (configurations.length > 0) {
            applySuggestionToForm(setValue, configurations[0], 0);
        }
    };
}
