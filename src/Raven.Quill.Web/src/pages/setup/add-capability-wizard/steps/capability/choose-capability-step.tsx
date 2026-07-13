import { useFormContext } from "react-hook-form";
import { FormRadioCards } from "@/components/form/form-radio-cards";
import type { AgentFormData } from "@/pages/setup/add-capability-wizard/capability-wizard-validation";
import { CAPABILITY_OPTIONS } from "@/pages/setup/add-capability-wizard/steps/capability/capability-options";

export function ChooseCapabilityStep() {
    const { control } = useFormContext<AgentFormData>();

    return (
        <FormRadioCards
            control={control}
            name="capability.type"
            options={CAPABILITY_OPTIONS}
            className="md:grid-cols-3"
        />
    );
}
