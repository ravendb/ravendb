/* eslint-disable react-refresh/only-export-components */
import type { WizardBodyComponentProps } from "@/components/form/wizard/form-wizard";
import { cn } from "@/lib/utils";
import { type AppFormData } from "@/pages/setup/add-app-wizard/wizard-model";
import { StepSection } from "@/pages/setup/add-app-wizard/wizard-step-section";
import { Bot, SlidersHorizontal } from "lucide-react";
import { useFormContext, useWatch } from "react-hook-form";

export function MapSchemaStep(props: WizardBodyComponentProps) {
    const { control } = useFormContext<AppFormData>();

    const mapSource = useWatch({
        control,
        name: "howToMap.source",
    });

    return (
        <StepSection {...props}>
            <div className="grid gap-5">
                <div className="grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
                    {MAPPING_MODE_OPTIONS.map((option) => {
                        const isSelected = option.value === mapSource;

                        return (
                            <button
                                key={option.value}
                                type="button"
                                disabled={option.disabled}
                                aria-pressed={isSelected}
                                className={cn(
                                    "min-h-32 rounded-lg border bg-background p-4 text-left transition-colors",
                                    "hover:bg-accent hover:text-accent-foreground",
                                    isSelected && "border-foreground bg-accent text-accent-foreground",
                                    option.disabled && "cursor-not-allowed opacity-55 hover:bg-background",
                                )}
                            >
                                {option.icon}
                                <span className="block text-sm font-semibold">{option.label}</span>
                                <span className="mt-2 block text-xs leading-5 text-muted-foreground">
                                    {option.description}
                                </span>
                            </button>
                        );
                    })}
                </div>
            </div>
        </StepSection>
    );
}

type MappingModeOption = {
    value: AppFormData["howToMap"]["source"];
    label: string;
    description: string;
    icon: React.ReactNode;
    disabled?: boolean;
};

const MAPPING_MODE_OPTIONS: MappingModeOption[] = [
    {
        value: "ai-suggested",
        label: "AI Suggest",
        description: "Suggest a mapping from application intent.",
        icon: <Bot className="mb-5 size-5" />,
    },
    {
        value: "manual",
        label: "Manual",
        description: "Build the mapping table by table.",
        icon: <SlidersHorizontal className="mb-5 size-5" />,
        disabled: true,
    },
];
