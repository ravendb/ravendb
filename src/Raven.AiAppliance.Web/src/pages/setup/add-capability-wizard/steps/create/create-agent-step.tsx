import { useFormContext, useWatch } from "react-hook-form";
import { cn } from "@/lib/utils";
import { FormTextarea } from "@/components/form/form-textarea";
import { Alert } from "@/components/shadcn/ui/alert";
import type { AiAgentConfiguration } from "@/api/generated/server-api";
import type { AgentFormData } from "@/pages/setup/add-capability-wizard/capability-wizard-validation";
import { useCapabilityWizardStore } from "@/pages/setup/add-capability-wizard/capability-wizard-store";

export function CreateAgentStep() {
    const { control, setValue } = useFormContext<AgentFormData>();
    const suggestions = useCapabilityWizardStore((state) => state.suggestions);
    const selectedIndex = useWatch({ control, name: "create.selectedIndex" });

    if (suggestions.length === 0) {
        return <Alert>Go back to the previous step to let AI analyze your data and suggest agents.</Alert>;
    }

    return (
        <div className="grid gap-6">
            <div className="grid gap-3">
                <h3 className="text-sm font-semibold">AI-suggested agents based on your data</h3>
                <div className="grid gap-3 md:grid-cols-3">
                    {suggestions.map((config, index) => {
                        const isSelected = index === selectedIndex;

                        return (
                            <button
                                key={config.identifier ?? index}
                                type="button"
                                aria-pressed={isSelected}
                                onClick={() => {
                                    setValue("create.selectedIndex", index);
                                    setValue("create.systemPrompt", config.systemPrompt ?? "", {
                                        shouldValidate: true,
                                    });
                                    setValue("review.name", config.name ?? "", { shouldValidate: true });
                                }}
                                className={cn(
                                    "min-h-28 rounded-lg border bg-background p-4 text-left transition-colors",
                                    "hover:bg-accent hover:text-accent-foreground",
                                    isSelected && "border-foreground bg-accent text-accent-foreground",
                                )}
                            >
                                <span className="block text-sm font-semibold">{config.name}</span>
                                <span className="mt-2 block text-xs leading-5 text-muted-foreground">
                                    {describeTools(config)}
                                </span>
                            </button>
                        );
                    })}
                </div>
            </div>

            <FormTextarea
                control={control}
                name="create.systemPrompt"
                label="What would you like your agent to do?"
                rows={6}
            />

            <div className="grid gap-3">
                <p className="text-center text-xs text-muted-foreground">or</p>
                <button
                    type="button"
                    disabled
                    className="min-h-16 cursor-not-allowed rounded-lg border bg-background p-4 text-left opacity-55"
                >
                    <span className="block text-sm font-semibold">Setup manually</span>
                    <span className="mt-2 block text-xs leading-5 text-muted-foreground">
                        Build the agent configuration from scratch. Coming soon.
                    </span>
                </button>
            </div>
        </div>
    );
}

function describeTools(config: AiAgentConfiguration) {
    const toolNames = (config.queries ?? []).map((query) => query.name).filter(Boolean);

    return toolNames.length > 0 ? `Tools: ${toolNames.join(", ")}` : "No query tools.";
}
