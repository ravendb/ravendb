import { useEffect } from "react";
import { Control, UseFormSetValue, useWatch } from "react-hook-form";
import { ClientConfigurationFormData } from "./ClientConfigurationValidation";

export default function useClientConfigurationFormController(
    control: Control<ClientConfigurationFormData>,
    setValue: UseFormSetValue<ClientConfigurationFormData>
): ClientConfigurationFormData {
    const formValues = useWatch({ control });

    useEffect(() => {
        if (!formValues.identityPartsSeparatorEnabled && formValues.identityPartsSeparatorValue !== null) {
            setValue("identityPartsSeparatorValue", null, { shouldValidate: true });
        }
        if (!formValues.maximumNumberOfRequestsEnabled && formValues.maximumNumberOfRequestsValue !== null) {
            setValue("maximumNumberOfRequestsValue", null, { shouldValidate: true });
        }
        if (!formValues.loadBalancerEnabled && formValues.loadBalancerValue !== "None") {
            setValue("loadBalancerValue", "None", { shouldValidate: true });
        }
        if (!formValues.loadBalancerEnabled && formValues.loadBalancerSeedEnabled !== false) {
            setValue("loadBalancerSeedEnabled", false, { shouldValidate: true });
        }
        if (!formValues.loadBalancerSeedEnabled && formValues.loadBalancerSeedValue !== null) {
            setValue("loadBalancerSeedValue", null, { shouldValidate: true });
        }
        if (!formValues.readBalanceBehaviorEnabled && formValues.readBalanceBehaviorValue !== "None") {
            setValue("readBalanceBehaviorValue", "None", { shouldValidate: true });
        }
    }, [formValues, setValue]);

    return formValues;
}
