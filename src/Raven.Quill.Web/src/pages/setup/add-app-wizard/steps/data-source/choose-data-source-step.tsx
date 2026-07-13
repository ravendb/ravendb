import { useFormContext } from "react-hook-form";
import { FormRadioCards } from "@/components/form/form-radio-cards";
import type { AppFormData } from "@/pages/setup/add-app-wizard/app-wizard-validation";
import { DATA_SOURCE_OPTIONS } from "@/pages/setup/add-app-wizard/steps/data-source/data-source-options";

export function ChooseDataSourceStep() {
    const { control } = useFormContext<AppFormData>();

    return (
        <FormRadioCards
            control={control}
            name="dataSource.source"
            options={DATA_SOURCE_OPTIONS}
            className="md:grid-cols-2"
        />
    );
}
