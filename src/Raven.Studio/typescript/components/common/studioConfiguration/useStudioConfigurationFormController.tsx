import { useEffect } from "react";
import { Control, UseFormSetValue, useWatch } from "react-hook-form";
import { StudioConfigurationFormData } from "./StudioConfigurationValidation";

export default function useStudioConfigurationFormController(
    control: Control<StudioConfigurationFormData>,
    setValue: UseFormSetValue<StudioConfigurationFormData>
): StudioConfigurationFormData {
    const formValues = useWatch({ control });

    useEffect(() => {
        if (!formValues.environmentTagValue && formValues.environmentTagValue !== null) {
            setValue("environmentTagValue", null, { shouldValidate: true });
        }
        if (!formValues.defaultReplicationFactorValue && formValues.defaultReplicationFactorValue !== null) {
            setValue("defaultReplicationFactorValue", null, { shouldValidate: true });
        }
        if (!formValues.collapseDocsWhenOpeningEnabled && formValues.collapseDocsWhenOpeningEnabled !== null) {
            setValue("collapseDocsWhenOpeningEnabled", null, { shouldValidate: true });
        }
        if (!formValues.sendAnonymousUsageDataEnabled && formValues.sendAnonymousUsageDataEnabled !== null) {
            setValue("sendAnonymousUsageDataEnabled", null, { shouldValidate: true });
        }
    }, [formValues, setValue]);

    return formValues;
}
