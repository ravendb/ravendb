import { useFormContext, useWatch } from "react-hook-form";
import { AiConsentGate, type AiConsentCopy } from "@/components/ai-consent/ai-consent-gate";
import { useAiConsent } from "@/components/ai-consent/use-ai-consent";
import { cn } from "@/lib/utils";
import {
    CARD_DESCRIPTION_CLASSES,
    CARD_LABEL_CLASSES,
    SELECTED_CARD_CLASSES,
} from "@/components/form/form-radio-cards";
import { FormTextarea } from "@/components/form/form-textarea";
import { Alert } from "@/components/shadcn/ui/alert";
import { Button } from "@/components/shadcn/ui/button";
import type { WizardBodyComponentProps } from "@/components/form/wizard/form-wizard";
import type { AgentFormData } from "@/pages/setup/add-capability-wizard/capability-wizard-validation";
import { AgentPromptProgress } from "@/pages/setup/add-capability-wizard/agent-prompt-progress";
import { SectionHeader } from "@/components/section-header";
import { Text } from "@/components/typography";
import { emptyAgentConfiguration } from "@/pages/setup/add-capability-wizard/agent-config-form";
import { useCapabilityWizardStore } from "@/pages/setup/add-capability-wizard/capability-wizard-store";
import { SuggestedAgentCardsSkeleton } from "@/pages/setup/add-capability-wizard/steps/create/suggested-agents-skeleton";
import { SuggestedAgentsProgress } from "@/pages/setup/add-capability-wizard/steps/create/suggested-agents-progress";
import { useSuggestedAgents } from "@/pages/setup/add-capability-wizard/steps/create/use-suggested-agents";
import { SuggestionPicker } from "@/pages/setup/add-capability-wizard/suggestion-picker";

const AGENT_CONSENT_COPY: AiConsentCopy = {
    gateDescription:
        "Building an agent with AI sends your collections and your description to the RavenDB AI service. Accept " +
        "its Terms of Use to unlock the two options below - or set the agent up manually instead.",
    dialogTitle: "Build your agent with AI",
    dialogDescription:
        "AI reads your collections to propose agents and turns your own description into a configuration you can " +
        "edit. That sends your data to the RavenDB AI service, so it is available only once you accept its Terms " +
        "of Use.",
};

export function CreateAgentStep({ isBusy }: WizardBodyComponentProps) {
    const { control, setValue } = useFormContext<AgentFormData>();
    const suggestions = useCapabilityWizardStore((state) => state.suggestions);
    const mode = useWatch({ control, name: "create.mode" });
    const {
        isSuggesting,
        startedAt: suggestionStartedAt,
        isConsentRequired,
        recheck: recheckSuggestions,
    } = useSuggestedAgents();
    const { isBlocked: isAiBlocked } = useAiConsent();

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
            {/* Reading the collections and turning a description into a configuration need the same
                consent, so both halves stay on screen while it is missing. */}
            <AiConsentGate variant="banner" copy={AGENT_CONSENT_COPY} />

            <div className="grid gap-3">
                <SectionHeader level="section" title="AI-suggested agents based on your data" />
                {isAiBlocked ? (
                    <SuggestedAgentCardsSkeleton isDisabled />
                ) : isSuggesting ? (
                    <SuggestedAgentsProgress startedAt={suggestionStartedAt} />
                ) : suggestions.length === 0 ? (
                    <Alert>
                        {isConsentRequired ? (
                            // The AI service refused the consent on file, so re-checking either loads
                            // the suggestions or brings the Terms of Use back to this step.
                            <>
                                The AI service refused the consent on file, so it could not read your collections.{" "}
                                <Button variant="link" className="h-auto p-0" onClick={recheckSuggestions}>
                                    Check again
                                </Button>
                            </>
                        ) : (
                            "AI could not suggest agents from your data. Describe your own below, or set one up manually."
                        )}
                    </Alert>
                ) : (
                    <SuggestionPicker />
                )}
            </div>

            <div
                className={cn(
                    "grid gap-3 rounded-lg border bg-background p-4 transition-colors",
                    isAiBlocked ? "opacity-55" : mode === "prompt" && SELECTED_CARD_CLASSES,
                )}
            >
                <FormTextarea
                    control={control}
                    name="create.promptInput"
                    label="Or describe what you'd like your agent to do"
                    description="When you click Next, AI generates an agent configuration from your description that you can review and edit."
                    placeholder="e.g. Help customers track their orders and answer questions about products."
                    rows={4}
                    disabled={isGenerating || isAiBlocked}
                    onFocus={choosePromptMode}
                />
                {isGenerating && <AgentPromptProgress />}
            </div>

            <div className="grid gap-3">
                <Text variant="caption" className="text-center">
                    or
                </Text>
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
