/* eslint-disable react-refresh/only-export-components */
import { useMutation } from "@tanstack/react-query";
import { Sparkles } from "lucide-react";
import { useFormContext, useWatch } from "react-hook-form";
import { api } from "@/api/api";
import { FormTextarea } from "@/components/form/form-textarea";
import type { WizardBodyComponentProps } from "@/components/form/wizard/form-wizard";
import { cn } from "@/lib/utils";
import { tablesSchema, type AppFormData } from "@/pages/setup/add-app-wizard/app-wizard-validation";
import { StepSection } from "@/pages/setup/add-app-wizard/wizard-step-section";

export function MapSchemaStep(props: WizardBodyComponentProps) {
    const { control, setValue } = useFormContext<AppFormData>();

    const mapSource = useWatch({
        control,
        name: "map.source",
    });

    const isAiSelected = mapSource === "ai-suggested";
    const isManualSelected = mapSource === "manual";
    const isPending = props.status === "pending";

    return (
        <StepSection {...props}>
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
                        disabled={isPending}
                        onClick={() => setValue("map.source", "ai-suggested")}
                        className={cn(
                            "block w-full rounded-t-lg p-4 text-left transition-colors",
                            !isAiSelected && "hover:bg-accent hover:text-accent-foreground",
                            isPending && "cursor-not-allowed",
                        )}
                    >
                        <Sparkles className="mb-4 size-5" aria-hidden="true" />
                        <span className="block text-sm font-semibold">AI Suggest</span>
                        <span className="mt-2 block text-xs leading-5 text-muted-foreground">
                            LLM proposes a draft CDCSinkConfiguration based on schema + your intent prompt.
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
                            disabled={!isAiSelected || isPending}
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
                    <span className="block text-sm font-semibold">Manual</span>
                    <span className="mt-2 block text-xs leading-5 text-muted-foreground">
                        Empty form scaffolded from the discovered schema. You pick what to flat / embed / link.
                    </span>
                </button>
            </div>
        </StepSection>
    );
}

export function useMapSchemaStep() {
    const { getValues, setValue } = useFormContext<AppFormData>();

    return useMutation({
        mutationFn: async () => {
            const { source, aiPrompt } = getValues("map");

            if (source !== "ai-suggested") {
                return true;
            }

            const result = await api.services.setup.suggestCdc({
                intentPrompt: aiPrompt.trim(),
            });

            if (result.status !== "Success" || !result.configuration) {
                throw new Error(
                    result.rationale.filter(Boolean).join("\n") || `AI suggestion failed (${result.status}).`,
                );
            }
            const tables = tablesSchema.parse(result.configuration.tables);
            setValue("mapAiSuggest.tables", tables);

            return true;
        },
    });
}
