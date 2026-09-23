import { ReactNode } from "react";
import { Control, FieldPath, FieldValues, PathValue, UseFormSetValue, useWatch } from "react-hook-form";
import { FormGroup, FormLabel, FormSwitch } from "components/common/Form";
import { Icon } from "components/common/Icon";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import { PopoverWithHoverProps } from "components/common/PopoverWithHover";
import useUniqueId from "components/hooks/useUniqueId";

interface ResetOnDisable<TFieldValues extends FieldValues, TValueName extends FieldPath<TFieldValues>> {
    setValue: UseFormSetValue<TFieldValues>;
    valueName: TValueName;
    resetValueTo?: PathValue<TFieldValues, TValueName>;
}

interface OverridableFieldProps<
    TFieldValues extends FieldValues,
    TOverrideName extends FieldPath<TFieldValues>,
    TValueName extends FieldPath<TFieldValues>,
> {
    control: Control<TFieldValues>;
    overrideName: TOverrideName;
    label: ReactNode;
    tooltip?: ReactNode;
    tooltipPlacement?: PopoverWithHoverProps["placement"];
    switchLabel?: ReactNode;
    disabled?: boolean;
    marginClass?: string;
    children: (state: { isOverridden: boolean }) => ReactNode;
    resetOnDisable?: ResetOnDisable<TFieldValues, TValueName>;
}

export default function OverridableField<
    TFieldValues extends FieldValues,
    TOverrideName extends FieldPath<TFieldValues>,
    TValueName extends FieldPath<TFieldValues> = FieldPath<TFieldValues>,
>({
    control,
    overrideName,
    label,
    tooltip,
    tooltipPlacement,
    switchLabel = "Override",
    disabled,
    marginClass,
    children,
    resetOnDisable,
}: OverridableFieldProps<TFieldValues, TOverrideName, TValueName>) {
    const isOverridden = !!useWatch({ control, name: overrideName });
    const labelId = useUniqueId("overridable-field-label-");

    return (
        <FormGroup marginClass={marginClass}>
            <FormLabel id={labelId}>
                {label}
                {tooltip && (
                    <PopoverWithHoverWrapper message={tooltip} placement={tooltipPlacement}>
                        <Icon icon="info-new" margin="ms-1" />
                    </PopoverWithHoverWrapper>
                )}
            </FormLabel>
            <div className="d-flex flex-wrap align-items-center">
                {children({ isOverridden })}
                <FormSwitch
                    control={control}
                    name={overrideName}
                    className="ms-2"
                    disabled={disabled}
                    aria-labelledby={labelId}
                    afterChange={(isChecked) => {
                        if (!isChecked && resetOnDisable) {
                            const {
                                setValue,
                                valueName,
                                resetValueTo = null as PathValue<TFieldValues, TValueName>,
                            } = resetOnDisable;
                            setValue(valueName, resetValueTo, { shouldValidate: true });
                        }
                    }}
                >
                    {switchLabel}
                </FormSwitch>
            </div>
        </FormGroup>
    );
}
