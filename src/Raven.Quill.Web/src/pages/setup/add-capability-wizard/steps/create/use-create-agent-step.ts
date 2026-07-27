import { useFormContext } from "react-hook-form";
import { useParams } from "react-router";
import type { AgentFormData } from "@/pages/setup/add-capability-wizard/capability-wizard-validation";
import type { WizardProgress } from "@/components/form/wizard/form-wizard";
import { suggestionToAgentConfiguration } from "@/pages/setup/add-capability-wizard/agent-config-form";
import { generateAgentFromPrompt } from "@/pages/setup/add-capability-wizard/agent-from-prompt";
import { useCapabilityWizardStore } from "@/pages/setup/add-capability-wizard/capability-wizard-store";

// Runs on "Next" from the create step. In "prompt" mode it generates an agent from the
// operator's description and seeds the editable review configuration. The data ("ai") and
// "manual" modes already hold their configuration, so they pass straight through.
// Throwing surfaces the failure inline via the wizard's advance error.
export function useCreateAgentStep() {
    const { slug = "" } = useParams();
    const { getValues, setValue } = useFormContext<AgentFormData>();
    const setPromptResult = useCapabilityWizardStore((state) => state.setPromptResult);

    return async (progress: WizardProgress) => {
        if (getValues("create.mode") !== "prompt") {
            return;
        }

        // The create-step validation guarantees a non-empty prompt in "prompt" mode.
        const prompt = getValues("create.promptInput").trim();

        // Skip regenerating when the text is unchanged since the last generation (e.g. the
        // operator went Back then Next again without editing the prompt).
        const cached = useCapabilityWizardStore.getState().promptResult;
        if (cached?.prompt === prompt) {
            return;
        }

        progress.report("Generating your agent...");
        const config = await generateAgentFromPrompt(slug, prompt);
        setPromptResult({ prompt, config });
        setValue("review", suggestionToAgentConfiguration(config), { shouldValidate: true });
    };
}
