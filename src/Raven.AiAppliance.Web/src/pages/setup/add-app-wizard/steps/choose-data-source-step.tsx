import { Database, DatabaseZap } from "lucide-react";
import { cn } from "@/lib/utils";
import { DATA_SOURCE_OPTIONS, type SetupWizardMessage } from "@/pages/setup/add-app-wizard/wizard-model";
import { StepSection } from "@/pages/setup/add-app-wizard/wizard-step-section";

export function ChooseDataSourceStep({ message }: { message?: SetupWizardMessage }) {
    return (
        <StepSection
            title="Choose data source"
            description="Where is the data this application will work with?"
            message={message}
        >
            <div className="grid gap-3 md:grid-cols-2">
                {DATA_SOURCE_OPTIONS.map((option) => {
                    const isSelected = option.id === "external";
                    const Icon = option.id === "external" ? Database : DatabaseZap;

                    return (
                        <button
                            key={option.id}
                            type="button"
                            disabled={option.disabled}
                            aria-pressed={isSelected}
                            className={cn(
                                "min-h-28 rounded-lg border bg-background p-4 text-left transition-colors",
                                "hover:bg-accent hover:text-accent-foreground",
                                isSelected && "border-foreground bg-accent text-accent-foreground",
                                option.disabled && "cursor-not-allowed opacity-55 hover:bg-background",
                            )}
                        >
                            <Icon className="mb-5 size-5" aria-hidden="true" />
                            <span className="block text-sm font-semibold">{option.label}</span>
                            <span className="mt-2 block text-xs leading-5 text-muted-foreground">
                                {option.description}
                            </span>
                        </button>
                    );
                })}
            </div>
        </StepSection>
    );
}
