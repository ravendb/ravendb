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
        if (!formValues.useSessionContextEnabled && formValues.loadBalancerSeedEnabled !== false) {
            setValue("loadBalancerSeedEnabled", false);
        }
        if (!formValues.loadBalancerSeedEnabled && formValues.loadBalancerSeedValue !== null) {
            setValue("loadBalancerSeedValue", null);
        }
    }, [formValues, setValue]);

    return formValues;
}
