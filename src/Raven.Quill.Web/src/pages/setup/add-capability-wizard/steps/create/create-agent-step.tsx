import { useFormContext, useWatch } from "react-hook-form";
import { AI_CONSENT_REQUIRED_MESSAGE } from "@/api/custom-services/assistant-service";
import { cn } from "@/lib/utils";
import {
    CARD_DESCRIPTION_CLASSES,
    CARD_LABEL_CLASSES,
    SELECTED_CARD_CLASSES,
} from "@/components/form/form-radio-cards";
import { FormTextarea } from "@/components/form/form-textarea";
import { Alert } from "@/components/shadcn/ui/alert";
import type { WizardBodyComponentProps } from "@/components/form/wizard/form-wizard";
import type { AgentFormData } from "@/pages/setup/add-capability-wizard/capability-wizard-validation";
import { AgentPromptProgress } from "@/pages/setup/add-capability-wizard/agent-prompt-progress";
import { emptyAgentConfiguration } from "@/pages/setup/add-capability-wizard/agent-config-form";
import { useCapabilityWizardStore } from "@/pages/setup/add-capability-wizard/capability-wizard-store";
import { SuggestedAgentsProgress } from "@/pages/setup/add-capability-wizard/steps/create/suggested-agents-progress";
import { useSuggestedAgents } from "@/pages/setup/add-capability-wizard/steps/create/use-suggested-agents";
import { SuggestionPicker } from "@/pages/setup/add-capability-wizard/suggestion-picker";

export function CreateAgentStep({ isBusy }: WizardBodyComponentProps) {
    const { control, setValue } = useFormContext<AgentFormData>();
    const suggestions = useCapabilityWizardStore((state) => state.suggestions);
    const mode = useWatch({ control, name: "create.mode" });
    const { isSuggesting, startedAt: suggestionStartedAt, isConsentRequired } = useSuggestedAgents();

    // "Next" in prompt mode is the generation call itself, so the wizard is only ever busy here
    // while the agent is being generated.
    const isGenerating = isBusy && mode === "prompt";

    const choosePromptMode = () => {
        if (mode !== "prompt") {
            setValue("create.mode", "prompt");
        }
    };

    const chooseManualSetup = () => {
        // Re-clicking must not wipe a manual configuration in progress.
        if (mode === "manual") {
            return;
        }

        setValue("create.mode", "manual");
        setValue("review", emptyAgentConfiguration());
    };

    return (
        <div className="grid gap-6">
            <div className="grid gap-3">
                <h3 className="text-sm font-semibold">AI-suggested agents based on your data</h3>
                {isSuggesting ? (
                    <SuggestedAgentsProgress startedAt={suggestionStartedAt} />
                ) : suggestions.length === 0 ? (
                    <Alert>
                        {isConsentRequired
                            ? AI_CONSENT_REQUIRED_MESSAGE
                            : "AI could not suggest agents from your data. Describe your own below, or set one up manually."}
                    </Alert>
                ) : (
                    <SuggestionPicker />
                )}
            </div>

            <div
                className={cn(
                    "grid gap-3 rounded-lg border bg-background p-4 transition-colors",
                    mode === "prompt" && SELECTED_CARD_CLASSES,
                )}
            >
                <FormTextarea
                    control={control}
                    name="create.promptInput"
                    label="Or describe what you'd like your agent to do"
                    description="When you click Next, AI generates an agent configuration from your description that you can review and edit."
                    placeholder="e.g. Help customers track their orders and answer questions about products."
                    rows={4}
                    disabled={isGenerating}
                    onFocus={choosePromptMode}
                />
                {isGenerating && <AgentPromptProgress />}
            </div>

            <div className="grid gap-3">
                <p className="text-center text-xs text-muted-foreground">or</p>
                <button
                    type="button"
                    aria-pressed={mode === "manual"}
                    onClick={chooseManualSetup}
                    className={cn(
                        "min-h-16 rounded-lg border bg-background p-4 text-left transition-colors",
                        mode !== "manual" && "hover:bg-accent hover:text-accent-foreground",
                        mode === "manual" && SELECTED_CARD_CLASSES,
                    )}
                >
                    <span className={cn("block", CARD_LABEL_CLASSES)}>Setup manually</span>
                    <span className={CARD_DESCRIPTION_CLASSES}>
                        Skip the AI suggestions and build the agent configuration from scratch in the next step.
                    </span>
                </button>
            </div>
        </div>
    );
}
