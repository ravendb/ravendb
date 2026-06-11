import { useFormContext, useWatch } from "react-hook-form";
import { cn } from "@/lib/utils";
import { FormTextarea } from "@/components/form/form-textarea";
import { Alert } from "@/components/shadcn/ui/alert";
import type { AgentFormData } from "@/pages/setup/add-capability-wizard/capability-wizard-validation";
import { emptyAgentConfiguration } from "@/pages/setup/add-capability-wizard/agent-config-form";
import { useCapabilityWizardStore } from "@/pages/setup/add-capability-wizard/capability-wizard-store";
import { SuggestionPicker } from "@/pages/setup/add-capability-wizard/suggestion-picker";

export function CreateAgentStep() {
    const { control, setValue } = useFormContext<AgentFormData>();
    const suggestions = useCapabilityWizardStore((state) => state.suggestions);
    const mode = useWatch({ control, name: "create.mode" });

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
                {suggestions.length === 0 ? (
                    <Alert>Go back to the previous step to let AI analyze your data and suggest agents.</Alert>
                ) : (
                    <SuggestionPicker />
                )}
            </div>

            {mode === "ai" && (
                <FormTextarea
                    control={control}
                    name="review.systemPrompt"
                    label="What would you like your agent to do?"
                    rows={6}
                />
            )}

            <div className="grid gap-3">
                <p className="text-center text-xs text-muted-foreground">or</p>
                <button
                    type="button"
                    aria-pressed={mode === "manual"}
                    onClick={chooseManualSetup}
                    className={cn(
                        "min-h-16 rounded-lg border bg-background p-4 text-left transition-colors",
                        "hover:bg-accent hover:text-accent-foreground",
                        mode === "manual" && "border-foreground bg-accent text-accent-foreground",
                    )}
                >
                    <span className="block text-sm font-semibold">Setup manually</span>
                    <span className="mt-2 block text-xs leading-5 text-muted-foreground">
                        Skip the AI suggestions and build the agent configuration from scratch in the next step.
                    </span>
                </button>
            </div>
        </div>
    );
}
