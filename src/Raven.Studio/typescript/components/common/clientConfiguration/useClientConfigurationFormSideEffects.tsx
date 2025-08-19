import { useEffect } from "react";
import { UseFormSetValue, UseFormWatch } from "react-hook-form";
import { ClientConfigurationFormData } from "./ClientConfigurationValidation";

export default function useClientConfigurationFormSideEffects(
    watch: UseFormWatch<ClientConfigurationFormData>,
    setValue: UseFormSetValue<ClientConfigurationFormData>
) {
    useEffect(() => {
        const { unsubscribe } = watch((values, { name }) => {
            if (name === "identityPartsSeparatorEnabled" && !values.identityPartsSeparatorEnabled) {
                setValue("identityPartsSeparatorValue", null, { shouldValidate: true });
            }
            if (name === "maximumNumberOfRequestsEnabled" && !values.maximumNumberOfRequestsEnabled) {
                setValue("maximumNumberOfRequestsValue", null, { shouldValidate: true });
            }
            if (name === "loadBalancerEnabled" && !values.loadBalancerEnabled) {
                setValue("loadBalancerValue", "None", { shouldValidate: true });
                setValue("loadBalancerSeedEnabled", false, { shouldValidate: true });
                setValue("loadBalancerSeedValue", null, { shouldValidate: true });
            }
            if (name === "loadBalancerValue" && values.loadBalancerValue === "None") {
                setValue("loadBalancerSeedEnabled", false, { shouldValidate: true });
                setValue("loadBalancerSeedValue", null, { shouldValidate: true });
            }
            if (name === "loadBalancerSeedEnabled" && !values.loadBalancerSeedEnabled) {
                setValue("loadBalancerSeedValue", null, { shouldValidate: true });
            }
            if (name === "readBalanceBehaviorEnabled" && !values.readBalanceBehaviorEnabled) {
                setValue("readBalanceBehaviorValue", "None", { shouldValidate: true });
            }
        });

        return () => unsubscribe();
    }, [watch, setValue]);
}
