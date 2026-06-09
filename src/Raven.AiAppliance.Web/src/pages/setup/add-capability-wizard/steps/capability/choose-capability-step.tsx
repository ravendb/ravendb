import { MessageSquare, ScanText, Sparkle } from "lucide-react";
import { useFormContext, useWatch } from "react-hook-form";
import { cn } from "@/lib/utils";
import type { AgentFormData } from "@/pages/setup/add-capability-wizard/capability-wizard-validation";

export function ChooseCapabilityStep() {
    const { control, setValue } = useFormContext<AgentFormData>();
    const selected = useWatch({ control, name: "capability.type" });

    return (
        <div className="grid gap-3 md:grid-cols-3">
            {CAPABILITY_OPTIONS.map((option) => {
                const isSelected = option.value === selected;

                return (
                    <button
                        key={option.value}
                        type="button"
                        disabled={option.isDisabled}
                        aria-pressed={isSelected}
                        onClick={() => option.value === "agent" && setValue("capability.type", "agent")}
                        className={cn(
                            "min-h-28 rounded-lg border bg-background p-4 text-left transition-colors",
                            "hover:bg-accent hover:text-accent-foreground",
                            isSelected && "border-foreground bg-accent text-accent-foreground",
                            option.isDisabled && "cursor-not-allowed opacity-55 hover:bg-background",
                        )}
                    >
                        {option.icon}
                        <span className="block text-sm font-semibold">{option.label}</span>
                        <span className="mt-2 block text-xs leading-5 text-muted-foreground">{option.description}</span>
                    </button>
                );
            })}
        </div>
    );
}

type CapabilityOption = {
    value: "agent" | "embeddings" | "genai";
    label: string;
    description: string;
    icon: React.ReactNode;
    isDisabled?: boolean;
};

const CAPABILITY_OPTIONS: CapabilityOption[] = [
    {
        value: "agent",
        label: "AI Agent",
        description:
            "Conversational agent grounded in live CDC data. System prompt + RQL tools. Deploy your agent to chosen channels.",
        icon: <MessageSquare className="mb-5 size-5" />,
    },
    {
        value: "embeddings",
        label: "Embeddings generation",
        description: "Vector index over CDC data. Collection + field selection for vectorisation.",
        isDisabled: true,
        icon: <ScanText className="mb-5 size-5" />,
    },
    {
        value: "genai",
        label: "GenAI",
        description: "Conversational agent grounded in live CDC data. System prompt + RQL tools.",
        isDisabled: true,
        icon: <Sparkle className="mb-5 size-5" />,
    },
];
