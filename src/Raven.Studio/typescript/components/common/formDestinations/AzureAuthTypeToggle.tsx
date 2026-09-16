import { Control, FieldPath, FieldValues } from "react-hook-form";
import { FormRadioToggleWithIcon } from "components/common/Form";
import { RadioToggleWithIconInputItem } from "components/common/toggles/RadioToggle";
import { AzureAuthType } from "components/common/formDestinations/utils/formDestinationsTypes";

interface AzureAuthTypeToggleProps<TFieldValues extends FieldValues> {
    control: Control<TFieldValues>;
    name: FieldPath<TFieldValues>;
    className?: string;
}

export default function AzureAuthTypeToggle<TFieldValues extends FieldValues>({
    control,
    name,
    className,
}: AzureAuthTypeToggleProps<TFieldValues>) {
    return (
        <FormRadioToggleWithIcon
            control={control}
            name={name}
            leftItem={accountKeyItem}
            rightItem={sasTokenItem}
            className={className}
        />
    );
}

const accountKeyItem: RadioToggleWithIconInputItem<AzureAuthType> = {
    label: "Account key",
    value: "accountKey",
    iconName: "key",
};

const sasTokenItem: RadioToggleWithIconInputItem<AzureAuthType> = {
    label: "SAS token",
    value: "sasToken",
    iconName: "lock",
};
