import { cn } from "@/lib/utils";
import type { AppFormData } from "@/pages/setup/add-app-wizard/app-wizard-validation";
import { DATA_SOURCE_OPTIONS } from "@/pages/setup/add-app-wizard/steps/data-source/data-source-options";
import { useFormContext, useWatch } from "react-hook-form";

export function ChooseDataSourceStep() {
    const { control } = useFormContext<AppFormData>();
    const source = useWatch({
        control,
        name: "dataSource.source",
    });

    return (
        <div className="grid gap-3 md:grid-cols-2">
            {DATA_SOURCE_OPTIONS.map((option) => {
                const isSelected = option.value === source;

                return (
                    <button
                        key={option.value}
                        type="button"
                        disabled={option.isDisabled}
                        aria-pressed={isSelected}
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
