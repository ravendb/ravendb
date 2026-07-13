import { useMutation } from "@tanstack/react-query";
import { useFormContext } from "react-hook-form";
import { useParams } from "react-router";
import { toast } from "sonner";
import type { AgentFormData } from "@/pages/setup/add-capability-wizard/capability-wizard-validation";
import { suggestionToAgentConfiguration } from "@/pages/setup/add-capability-wizard/agent-config-form";
import { generateAgentFromPrompt } from "@/pages/setup/add-capability-wizard/agent-from-prompt";
import { useCapabilityWizardStore } from "@/pages/setup/add-capability-wizard/capability-wizard-store";

// Regenerates the agent from the edited prompt in the review step, replacing the current
// configuration (and the stored prompt result) with the new candidate.
export function useRegenerateAgentFromPromptMutation() {
    const { slug = "" } = useParams();
    const { setValue } = useFormContext<AgentFormData>();
    const setPromptResult = useCapabilityWizardStore((state) => state.setPromptResult);

    return useMutation({
        mutationFn: (prompt: string) => generateAgentFromPrompt(slug, prompt),
        onSuccess: (config, prompt) => {
            setPromptResult({ prompt, config });
            setValue("review", suggestionToAgentConfiguration(config), { shouldValidate: true });
        },
        onError: (error) => {
            // Backend rejections can carry a full stack trace; show only the first line.
            const message = error instanceof Error ? error.message.split("\n")[0] : "Could not regenerate agent.";
            toast.error(message);
        },
    });
}
