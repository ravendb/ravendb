import { useFormContext } from "react-hook-form";
import { useParams } from "react-router";
import { api } from "@/api/api";
import type { AgentFormData } from "@/pages/setup/add-capability-wizard/capability-wizard-validation";
import { applySuggestionToForm } from "@/pages/setup/add-capability-wizard/agent-config-form";
import { useCapabilityWizardStore } from "@/pages/setup/add-capability-wizard/capability-wizard-store";

export function useConnectProviderStep() {
    const { slug = "" } = useParams();
    const { setValue } = useFormContext<AgentFormData>();
    const setSuggestions = useCapabilityWizardStore((state) => state.setSuggestions);
    const suggestions = useCapabilityWizardStore((state) => state.suggestions);

    return async () => {
        // Already fetched (e.g. the operator went Back then Next again) — keep their edits
        // instead of clobbering the form with a fresh suggestion.
        if (suggestions.length > 0) {
            return;
        }

        const result = await api.services.apps.suggestAgent(slug, {
            mode: "from-data",
            intentPrompt: null,
        });

        if (result.status !== "Success" || result.configurations.length === 0) {
            throw new Error(result.rationale?.filter(Boolean).join("\n") || `AI suggestion failed (${result.status}).`);
        }

        setSuggestions(result.configurations);
        applySuggestionToForm(setValue, result.configurations[0], 0);
    };
}
