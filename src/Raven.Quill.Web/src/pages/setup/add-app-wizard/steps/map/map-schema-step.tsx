import { Sparkles } from "lucide-react";
import { useFormContext } from "react-hook-form";
import { FormRadioCards } from "@/components/form/form-radio-cards";
import { FormTextarea } from "@/components/form/form-textarea";
import type { WizardBodyComponentProps } from "@/components/form/wizard/form-wizard";
import { useSetupWizardStore } from "@/pages/setup/add-app-wizard/app-wizard-store";
import { type AppFormData } from "@/pages/setup/add-app-wizard/app-wizard-validation";
import { ImportedConfigAlert } from "@/pages/setup/add-app-wizard/imported-config-alert";
import { AI_SUGGEST_OPTION, MANUAL_OPTION } from "@/pages/setup/add-app-wizard/steps/map/map-source-options";

export function MapSchemaStep({ isBusy }: WizardBodyComponentProps) {
    const { control } = useFormContext<AppFormData>();
    const isLocked = useSetupWizardStore((state) => state.importState) === "locked";
    const isDisabled = isBusy || isLocked;

    return (
        <div className="grid gap-5">
            <ImportedConfigAlert />
            <FormRadioCards
                control={control}
                name="map.source"
                disabled={isDisabled}
                className="gap-4"
                options={[
                    {
                        value: AI_SUGGEST_OPTION.value,
                        label: AI_SUGGEST_OPTION.label,
                        description: AI_SUGGEST_OPTION.description,
                        icon: <Sparkles className="size-5" aria-hidden="true" />,
                        content: ({ select }) => (
                            <FormTextarea
                                control={control}
                                name="map.aiPrompt"
                                label={
                                    <>
                                        Intent prompt
                                        <span className="text-xs font-normal text-muted-foreground">(optional)</span>
                                    </>
                                }
                                placeholder='e.g. "Embed line items in each order, link customers by id, flatten addresses."'
                                rows={4}
                                disabled={isDisabled}
                                onFocus={select}
                            />
                        ),
                    },
                    {
                        value: MANUAL_OPTION.value,
                        label: MANUAL_OPTION.label,
                        description: MANUAL_OPTION.description,
                    },
                ]}
            />
        </div>
    );
}
