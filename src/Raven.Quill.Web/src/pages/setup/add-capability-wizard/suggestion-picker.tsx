import { useFormContext, useWatch } from "react-hook-form";
import { cn } from "@/lib/utils";
import {
    CARD_DESCRIPTION_CLASSES,
    CARD_LABEL_CLASSES,
    SELECTED_CARD_CLASSES,
} from "@/components/form/form-radio-cards";
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
        <div className="grid auto-cols-[minmax(0,1fr)] grid-flow-col gap-3">
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
                            "flex min-h-28 flex-col items-start rounded-lg border bg-background p-4 text-left transition-colors",
                            !isSelected && "hover:bg-accent hover:text-accent-foreground",
                            isSelected && SELECTED_CARD_CLASSES,
                        )}
                    >
                        <span className={cn("block", CARD_LABEL_CLASSES)}>{config.name}</span>
                        <span className={cn(CARD_DESCRIPTION_CLASSES, "line-clamp-4")}>{config.systemPrompt}</span>
                    </button>
                );
            })}
        </div>
    );
}
