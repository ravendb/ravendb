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
            setValue("identityPartsSeparatorValue", null);
        }
        if (!formValues.maximumNumberOfRequestsEnabled && formValues.maximumNumberOfRequestsValue !== null) {
            setValue("maximumNumberOfRequestsValue", null);
        }
        if (!formValues.loadBalancerEnabled && formValues.loadBalancerValue !== "None") {
            setValue("loadBalancerValue", "None");
        }
        if (!formValues.loadBalancerEnabled && formValues.loadBalancerSeedEnabled !== false) {
            setValue("loadBalancerSeedEnabled", false);
        }
        if (!formValues.loadBalancerSeedEnabled && formValues.loadBalancerSeedValue !== null) {
            setValue("loadBalancerSeedValue", null);
        }
        if (!formValues.readBalanceBehaviorEnabled && formValues.readBalanceBehaviorValue !== "None") {
            setValue("readBalanceBehaviorValue", "None");
        }
    }, [formValues, setValue]);

    return formValues;
}
