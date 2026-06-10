import { Sparkles } from "lucide-react";
import { useFormContext, useWatch } from "react-hook-form";
import { FormTextarea } from "@/components/form/form-textarea";
import type { WizardBodyComponentProps } from "@/components/form/wizard/form-wizard";
import { cn } from "@/lib/utils";
import { type AppFormData } from "@/pages/setup/add-app-wizard/app-wizard-validation";
import { AI_SUGGEST_OPTION, MANUAL_OPTION } from "@/pages/setup/add-app-wizard/steps/map/map-source-options";

export function MapSchemaStep({ isBusy }: WizardBodyComponentProps) {
    const { control, setValue } = useFormContext<AppFormData>();

    const mapSource = useWatch({
        control,
        name: "map.source",
    });

    const isAiSelected = mapSource === "ai-suggested";
    const isManualSelected = mapSource === "manual";

    return (
        <div className="grid gap-4">
            <div
                className={cn(
                    "rounded-lg border bg-background transition-colors",
                    isAiSelected && "border-foreground bg-accent text-accent-foreground",
                )}
            >
                <button
                    type="button"
                    aria-pressed={isAiSelected}
                    disabled={isBusy}
                    onClick={() => setValue("map.source", "ai-suggested")}
                    className={cn(
                        "block w-full rounded-t-lg p-4 text-left transition-colors",
                        !isAiSelected && "hover:bg-accent hover:text-accent-foreground",
                        isBusy && "cursor-not-allowed",
                    )}
                >
                    <Sparkles className="mb-4 size-5" aria-hidden="true" />
                    <span className="block text-sm font-semibold">{AI_SUGGEST_OPTION.label}</span>
                    <span className="mt-2 block text-xs leading-5 text-muted-foreground">
                        {AI_SUGGEST_OPTION.description}
                    </span>
                </button>
                <div className="px-4 pb-4">
                    <FormTextarea
                        control={control}
                        name="map.aiPrompt"
                        label={
                            <>
                                Intent prompt <span className="font-normal text-muted-foreground">(optional)</span>
                            </>
                        }
                        placeholder='e.g. "Embed line items in each order, link customers by id, flatten addresses."'
                        rows={4}
                        disabled={!isAiSelected || isBusy}
                    />
                </div>
            </div>

            <button
                type="button"
                aria-pressed={isManualSelected}
                disabled
                className={cn(
                    "min-h-24 cursor-not-allowed rounded-lg border bg-background p-4 text-left opacity-55 transition-colors",
                    isManualSelected && "border-foreground bg-accent text-accent-foreground",
                )}
            >
                <span className="block text-sm font-semibold">{MANUAL_OPTION.label}</span>
                <span className="mt-2 block text-xs leading-5 text-muted-foreground">{MANUAL_OPTION.description}</span>
            </button>
        </div>
    );
}
