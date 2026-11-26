import { Control, UseFormSetValue } from "react-hook-form";
import { DIRTY_AND_VALIDATE } from "components/common/Form";
import { FormSelectWithDefault } from "components/common/FieldWithDefault";
import { LoadBalanceBehaviorTooltip } from "components/common/clientConfiguration/ClientConfigurationTooltips";
import ClientConfigurationUtils from "components/common/clientConfiguration/ClientConfigurationUtils";
import { ClientConfigurationFormData } from "components/common/clientConfiguration/ClientConfigurationValidation";
import LoadBalanceBehavior = Raven.Client.Http.LoadBalanceBehavior;

interface LoadBalanceBehaviorFieldProps {
    control: Control<ClientConfigurationFormData>;
    setValue: UseFormSetValue<ClientConfigurationFormData>;
    disabled?: boolean;
}

export default function LoadBalanceBehaviorField({ control, setValue, disabled }: LoadBalanceBehaviorFieldProps) {
    const clearSeedUnlessSessionContext = (loadBalanceBehavior: LoadBalanceBehavior) => {
        if (loadBalanceBehavior !== "UseSessionContext") {
            setValue("loadBalancerSeedValue", null, DIRTY_AND_VALIDATE);
        }
    };

    return (
        <FormSelectWithDefault
            control={control}
            name="loadBalancerValue"
            tooltipPlacement="right"
            label="Load Balance Behavior"
            tooltip={<LoadBalanceBehaviorTooltip />}
            disabled={disabled}
            afterChange={clearSeedUnlessSessionContext}
            defaultValueLabel="None"
            options={ClientConfigurationUtils.getLoadBalanceBehaviorOptions()}
            isSearchable={false}
        />
    );
}
