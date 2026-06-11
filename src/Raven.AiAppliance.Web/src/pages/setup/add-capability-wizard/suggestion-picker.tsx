import { useFormContext, useWatch } from "react-hook-form";
import { cn } from "@/lib/utils";
import type { AiAgentConfiguration } from "@/api/generated/server-api";
import type { AgentFormData } from "@/pages/setup/add-capability-wizard/capability-wizard-validation";
import { applySuggestionToForm } from "@/pages/setup/add-capability-wizard/agent-config-form";
import { useCapabilityWizardStore } from "@/pages/setup/add-capability-wizard/capability-wizard-store";

// Cards for the AI-suggested agent candidates. Picking a card seeds the editable
// configuration in the review step, replacing any edits made so far.
export function SuggestionPicker() {
    const { control, setValue } = useFormContext<AgentFormData>();
    const suggestions = useCapabilityWizardStore((state) => state.suggestions);
    const mode = useWatch({ control, name: "create.mode" });
    const selectedIndex = useWatch({ control, name: "create.selectedIndex" });

    return (
        <div className="grid gap-3 md:grid-cols-3">
            {suggestions.map((config, index) => {
                const isSelected = mode === "ai" && index === selectedIndex;

                return (
                    <button
                        key={config.identifier ?? index}
                        type="button"
                        aria-pressed={isSelected}
                        onClick={() => {
                            // Re-clicking the selected card must not reseed (it would
                            // discard edits made in the review step).
                            if (!isSelected) {
                                applySuggestionToForm(setValue, config, index);
                            }
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
    );
}

function describeTools(config: AiAgentConfiguration) {
    const toolNames = (config.queries ?? []).map((query) => query.name).filter(Boolean);

    return toolNames.length > 0 ? `Tools: ${toolNames.join(", ")}` : "No query tools.";
}
